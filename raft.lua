local string = string
local table = table
local math = math

local g = require('global')
local log = require('log')
local raftLog = require('raftLog')
local tracker = require('tracker.tracker')
local readOnly = require('readOnly')
local restore = require('confchange.restore')

local stateType = {
	FOLLOWER = 0;
	CANDIDATE = 1;
	LEADER = 2;
	PRECANDIDATE = 3;
}

local stateText = {
	[0] = 'stateFollower';
	[1] = 'stateCandidate';
	[2] = 'stateLeader';
	[3] = 'statePreCandidate';
}

local campaignType = {
	PREELECTION = 'campaignPreElection';
	ELECTION = 'campaignElection';
	TRANSFER = 'campaignTransfer';
}

local config = {
	-- ID num
	-- electionTick num
	-- heartbeatTick num
	-- storage storage
	-- applied num
	-- maxSizePerMsg num
	-- maxCommittedSizePerReady num
	-- maxUncommittedEntriesSize num
	-- maxInflightMsgs num
	-- checkQuorum bool
	-- preVote bool
	-- readOnlyOption readOnlyOption
	-- disableProposalForwarding bool
}
local this = config
this.__index = this

function this:validate()
	if self.ID == g.none then
		return 'cannot use none as id'
	end
	if self.heartbeatTick <= 0 then
		return 'heartbeat tick must be greater than 0'
	end
	if self.electionTick <= self.heartbeatTick then
		return 'election tick must be greater than heartbeat tick'
	end
	if not self.storage then
		return 'storage cannot be nil'
	end
	if self.maxUncommittedEntriesSize == 0 then
		self.maxUncommittedEntriesSize = g.noLimit
	end
	if self.maxCommittedSizePerReady == 0 then
		self.maxCommittedSizePerReady = self.maxSizePerMsg
	end
	if self.maxInflightMsgs <= 0 then
		return 'max inflight messages must be greater than 0'
	end
	if self.readOnlyOption == readOnlyOption.LEASE_BASED and not self.checkQuorum then
		return 'checkQuorum must be enabled when readOnlyOption is LEASE_BASED'
	end
end

local raft = {
	-- id num
	-- term num
	-- vote num
	-- readStates table[]readState
	-- raftLog raftLog
	-- maxMsgSize num
	-- maxUncommittedSize num
	-- prs progressTracker
	-- state stateType
	-- isLearner bool
	-- msgs table[]message
	-- lead num
	-- leadTransferee num
	-- pendingConfIndex num
	-- uncommittedSize num
	-- readOnly readOnly
	-- electionElapsed num
	-- heartbeatElapsed num
	-- checkQuorum bool
	-- preVote bool
	-- heartbeatTimeout num
	-- electionTimeout num
	-- randomizedElectionTimeout num
	-- disableProposalForwarding bool
	-- tick function()
	-- step function(raft, message) error
	-- pendingReadIndexMessages table[]message
}
this = raft
this.__index = this

this.newRaft = function(c)
	local err = c:validate()
	if err then
		g.panic(err)
	end
	local hs, cs = c.storage:initialState()
	local r = {
		id = c.ID;
		lead = g.none;
		raftLog = raftLog.newLog(c.storage, c.maxCommittedSizePerReady);
		maxMsgSize = c.maxSizePerMsg;
		maxUncommittedSize = c.maxUncommittedEntriesSize;
		prs = tracker.new(c.maxInflightMsgs);
		electionTimeout = c.electionTick;
		heartbeatTimeout = c.heartbeatTick;
		checkQuorum = c.checkQuorum;
		preVote = c.preVote;
		readOnly = readOnly.newReadOnly(c.readOnlyOption);
		disableProposalForwarding = c.disableProposalForwarding;
	}
	local cfg, prs, err = restore.restore({
		tracker = r.prs;
		lastIndex = r.raftLog:lastIndex();
	}, cs)
	if err then
		g.panic(err)
	end
	local cs2 = r:switchToConfig(cfg, prs)
	if not g.table_compare(cs, cs2) then
		g.panic(string.format('confState not equivalent after sorting:%s, inputs were:%s', table.toJson(cs), table.toJson(cs2)))
	end
	if next(hs) then
		r:loadState(hs)
	end
	if c.applied > 0 then
		r.raftLog:appliedTo(c.applied)
	end
	r:becomeFollower(r.term, g.none)
	local nodesStrs = {}
	for i, id in ipairs(r.prs:voterNodes()) do
		if i == 1 then
			nodesStrs = nodesStrs .. id
		else
			nodesStrs = nodesStrs .. ', ' .. id
		end
	end
	log.print(string.format('newRaft %d [peers:[%s], term:%d, commit:%d, applied:%d, lastIndex:%d, lastTerm:%d]',
		r.id, nodesStrs, r.term, r.raftLog.committed, r.raftLog.applied, r.raftLog:lastIndex(), r.raftLog:lastTerm()))
	return r
end

function this:hasLeader()
	return r.lead ~= g.none
end

function this:softState()
	return {lead = r.lead, raftState:r.state}
end

function this:hardState()
	return {term = r.term, vote = r.vote, commit = r.raftLog.committed}
end

function this:send(m)
	if m.from == g.none then
		m.from = self.id
	end
	if m.type == g.messageType.VOTE or m.type == g.messageType.VOTE_RESP or m.type == g.messageType.PREVOTE or m.type == g.messageType.PREVOTE_RESP then
		if m.term == 0 then
			g.panic('term should be set when sending ' .. m.type)
		end
	else
		if m.term ~= 0 then
			panic('term should not be set when sending ' .. m.type .. ', term was ' .. m.term)
		end
		if m.type ~= g.messageType.PROP and m.type ~= g.messageType.READ_INDEX then
			m.term = r.term
		end
	end
	r.msgs[#r.msgs + 1] = m
end

function this:sendAppend(to)
	self:maybeSendAppend(to, true)
end

function this:maybySendAppend(to, sendIfEmpty)
	local pr = self.prs.progress[to]
	if pr:isPaused() then
		return false
	end
	local m = {to = to}
	local term, errt = self.raftLog.term(self.next - 1)
	local ents, erre = self.raftLog.entries(pr.next, self.maxMsgSize)
	if #ents == 0 and not sendIfEmpty then
		return false
	end
	if errt or erre then
		if not pr.recentActive then
			log.print('ignore sending snapshot to ' .. to .. ' since it is not recently active')
		end
		m.type = g.messageType.SNAP
		local snapshot = self.raftLog:snapshot()
		m.snapshot = snapshot
		local sindex, sterm = snapshot.metadata.index, snapshot.metadata.term
		local str = pr.string()
		log.print(string.format('%d [firstIndex:%d, commit:%d] send snapshot[index:%d, term:%d] to %d [%s]',
			self.id, self.raftLog.firstIndex(), self.raftLog.committed, sindex, sterm, to, str))
		pr:becomeSnapshot(sindex)
		log.print(string.format('%d paused sending replication messages to %d [%s]', self.id, to, str))
	else
		m.type = g.messageType.APP
		m.index = pr.next - 1
		m.logTerm = term
		m.entries = ents
		m.commit = self.raftLog.committed
		if #m.entries ~= 0 then
			if pr.state == g.stateType.REPLICATE then
				local last = m.entries[#m.entries].index
				pr:optimisticUpdate(last)
				pr.inflights:add(last)
			elseif pr.state == g.stateType.PROBE then
				pr.probeSent = true
			else
				g.panic(self.id .. ' is sending append in unhandled state ' .. pr.state)
			end
		end
	end
	self:send(m)
	return true
end

function this:sendHeartbeat(to, ctx)
	local commit = math.min(self.prs.progress[to].match, self.raftLog.committed)
	self:send({
		to = to;
		type = g.messageType.HEARTBEAT;
		commit = commit;
		context = ctx;
	})
end

function this:bcastAppend()
	self.prs.visit(function(id)
		if id == self.id then
			return
		end
		self:sendAppend(id)
	end)
end

function this:bcastHeartbeat()
	local lastCtx = self.readOnly:lastPendingRequestCtx()
	self:bcastHeartbeatWithCtx(#lastCtx ~= 0 and lastCtx or nil)
end

function this:bcastHeartbeatWithCtx(ctx)
	self.prs:visit(function(id)
		if id == self.id then
			return
		end
		self:sendHeartbeat(id, ctx)
	end)
end

function this:advance(rd)
	self:reduceUncommittedSize(rd.committedEntries)
	local newApplied = rd:appliedCursor()
	if newApplied > 0 then
		local oldApplied = self.raftLog.applied
		self.raftLog:appliedTo(newApplied)
		if self.prs.config.autoLeave and oldApplied <= self.pendingConfIndex and newApplied >= self.pendingConfIndex and self.state == stateType.LEADER then
			local ent = {type = g.entryType.CONFCHANGE}
			if not self:appendEntry(ent) then
				g.panic('refused un-refusable auto-leaving confChange')
			end
			self.pendingConfIndex = self.raftLog:lastIndex()
			log.print('initiating automatic transition out of joint configuration ' .. self.prs:string())
		end
	end
	if #rd.entries > 0 then
		local e = rd.entries[#rd.entries]
		self.raftLog.stableTo(e.index, e.term)
	end
end

return this