local string = string
local table = table
local math = math

local g = require('global')
local log = require('log')
local raftLog = require('raftLog')
local tracker = require('tracker.tracker')
local readOnly = require('readOnly')
local restore = require('confchange.restore')
local util = require('util')

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

local function stepLeader(r--[[raft]], m--[[message]])
	if m.type == g.msgType.BEAT then
		r:bcastHeartbeat()
		return nil
	elseif m.type == g.msgType.CHECK_QUORUM then
		local pr = r.prs.progress[r.id]
		if pr then pr.recentActive = true end
		if not r.prs:quorumActive() then
			log.warning(r.id .. ' stepped down to follower since quorum is not active')
			r:becomeFollower(r.term, g.none)
		end
		r.prs:visit(function(id, pr)
			if id ~= r.id then
				pr.recentActive = false
			end
		end)
		return nil
	elseif m.type == g.msgType.PROP then
		if #m.entries == 0 then log.panic(r.id .. ' stepped empty msgProp') end
		if not r.prs.progress[r.id] then return g.errCode.PROPOSAL_DROPPED end
		if r.leadTransferee ~= g.none then
			log.debug('%d [term %d] transfer leadership to %d is in progress; dropping proposal', r.id, r.term, r.leadTransferee)
			return g.errCode.PROPOSAL_DROPPED
		end
		for i, e in ipairs(m.entries) do
			local cc
			if e.type == g.entryType.CONFCHANGE then
				cc = g.bytesToTable(e.data)
				if not cc then g.panic('parse entry.data failed') end
			end
			if cc then
				local alreadyPending = r.pendingConfIndex > r.raftLog.applied
				local alreadyJoint = #r.prs.config.voters[2] > 0
				local wantsLeaveJoint = #cc.changes == 0
				local refused
				if alreadyPending then
					refused = string.format('possible unapplied conf change at index %d (applied to %d)', r.pendingConfIndex, r.raftLog.applied)
				elseif alreadyJoint and not wantsLeaveJoint then
					refused = 'must transition out of joint config first'
				elseif not alreadyJoint and wantsLeaveJoint then
					refused = 'not in joint state; refusing empty conf change'
				end
				if #refused > 0 then
					log.info(string.format('%d ignoring conf change %s at config: %s: %s',
						r.id, table.toJson(cc), r.prs.config, refused))
					m.entries[i] = {type = g.entryType.NORMAL}--[[entry]]
				else
					r.pendingConfIndex = r.raftLog:lastIndex() + i
				end
			end
		end
		if not r.appendEntry(unpack(m.entries)) then
			return g.errCode.PROPOSAL_DROPPED
		end
		r:bcastAppend()
		return nil
	elseif m.type == g.msgType.READ_INDEX then
		if r.prs:isSingleton() then
			local resp = r:responseToReadIndexReq(m, r.raftLog.committed)
			if resp then r.send(resp) end
			return nil
		end
		if not r:committedEntryInCurrentTerm() then
			r.pendingReadIndexMessages[#r.pendingReadIndexMessages+1] = m
			return nil
		end
		sendMsgReadIndexResponse(r, m)
		return nil
	end

	local pr = r.prs.progress[m.from]
	if not pr then
		log.debug(r.id .. ' no progress avaliable for ' .. m.from)
		return nil
	end
	if m.type == g.msgType.APP_RESP then
		pr.recentActive = true
		if m.reject then
			log.debug(string.format('%d received msgAppResp(rejected, hint: (index %d, term %d)) from %d for index %d',
				r.id, m.rejectHint, m.logTerm, m.from, m.index))
			local nextProbeIdx = m.rejectHint
			if m.logTerm > 0 then
				nextProbeIdx = r.raftLog.findConflictByTerm(m.rejectHint, m.logTerm)
			end
			if pr.maybeDecrTo(m.index, nextProbeIdx) then
				log.debug(string.format('%d decreased progress of %d to [%s]',
					r.id, m.from, pr))
				if pr.state == g.stateType.REPLICATE then
					pr:becomeProbe()
				end
				r:sendAppend(m.from)
			end
		else
			local oldPaused = pr:isPaused()
			if pr:maybeUpdate(m.index) then
				if pr.state == g.stateType.PROBE then
					pr:becomeReplicate()
				elseif pr.state == g.stateType.SNAPSHOT and pr.match >= pr.pendingSnapshot then
					log.debug(string.format('%d recovered from needing snapshot, resumed sending replication messages to %d [%s]', r.id, m.from, pr))
					pr:becomeReplicate()
				elseif pr.state == g.stateType.REPLICATE then
					pr.inflights:freeLE(m.index)
				end
				if r:maybeCommit() then
					reasePendingReadIndexMessages(r)
					r:bcastAppend()
				elseif oldPaused then
					r:sendAppend(m.from)
				end
				while r:maybySendAppend(m.from, false) do end
				if m.from == r.leadTransferee and pr.match == r.raftLog:lastIndex() then
					log.info(r.id .. ' sent msgTimeoutNow to ' .. m.from .. ' after received msgAppResp')
					r:sendTimeoutNow(m.from)
				end
			end
		end
	elseif m.type == g.msgType.HEARTBEAT_RESP then
		pr.recentActive = true
		pr.probeSent = false
		if pr.state == stateType.REPLICATE and pr.inflights:full() then
			pr.inflights:freeFirstOne()
		end
		if pr.match < r.raftLog:lastIndex() then
			r:sendAppend(m.from)
		end
		if r.readOnly.option ~= g.readOnlyOption.SAFE or #m.context == 0 then
			return
		end
		if r.prs.voters:voteResult(r.readOnly.recvAck(m.from, m.context)) ~= g.voteResult.WON then
			return
		end
		local rss = r.readOnly:advance(m)
		for _, rs in ipairs(rss) do
			local resp = r:responseToReadIndexReq(rs.req, rs.index)
			if resp.to ~= g.none then
				r:send(resp)
			end
		end
	elseif m.type == g.msgType.SNAP_STATUS then
			if pr.state ~= stateType.SNAPSHOT then
				return nil
			end
			if not m.reject then
				
			else

			end
	elseif m.type == g.msgType.UNREACHABLE then

	elseif m.type == g.msgType.TRANSFER_LEADER then

	end
end

this.newRaft = function(c--[[config]])
	local err = c:validate()
	if err then
		log.panic(err)
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
		readOnly = readOnly.new(c.readOnlyOption);
		disableProposalForwarding = c.disableProposalForwarding;
	}
	local cfg, prs, err = restore.restore({
		tracker = r.prs;
		lastIndex = r.raftLog:lastIndex();
	}, cs)
	if err then
		log.panic(err)
	end
	local cs2 = r:switchToConfig(cfg, prs)
	if not g.table_compare(cs, cs2) then
		log.panic(string.format('confState not equivalent after sorting:%s, inputs were:%s', table.toJson(cs), table.toJson(cs2)))
	end
	if next(hs) then
		r:loadState(hs)
	end
	if c.applied > 0 then
		r.raftLog:appliedTo(c.applied)
	end
	r:becomeFollower(r.term, g.none)
	local nodesStrs = ''
	for i, id in ipairs(r.prs:voterNodes()) do
		if i == 1 then
			nodesStrs = nodesStrs .. id
		else
			nodesStrs = nodesStrs .. ', ' .. id
		end
	end
	log.info(string.format('newRaft %d [peers:[%s], term:%d, commit:%d, applied:%d, lastIndex:%d, lastTerm:%d]',
		r.id, nodesStrs, r.term, r.raftLog.committed, r.raftLog.applied, r.raftLog:lastIndex(), r.raftLog:lastTerm()))
	return r
end

function this:hasLeader()
	return self.lead ~= g.none
end

function this:softState()
	return {lead = self.lead, raftState = self.state}
end

function this:hardState()
	return {term = self.term, vote = self.vote, commit = self.raftLog.committed}
end

function this:send(m--[[message]])
	if m.from == g.none then
		m.from = self.id
	end
	if m.type == g.msgType.VOTE or m.type == g.msgType.VOTE_RESP or m.type == g.msgType.PREVOTE or m.type == g.msgType.PREVOTE_RESP then
		if not m.term then
			log.panic('term should be set when sending ' .. m.type)
		end
	else
		if m.term then
			panic('term should not be set when sending ' .. m.type .. ', term was ' .. m.term)
		end
		if m.type ~= g.msgType.PROP and m.type ~= g.msgType.READ_INDEX then
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
	local m = {}--[[message]]
	m.to = to
	local term, errt = self.raftLog:term(self.next - 1)
	local ents, erre = self.raftLog:entries(pr.next, self.maxMsgSize)
	if #ents == 0 and not sendIfEmpty then
		return false
	end
	if errt or erre then
		if not pr.recentActive then
			log.debug('ignore sending snapshot to ' .. to .. ' since it is not recently active')
			return false
		end
		m.type = g.msgType.SNAP
		local snapshot = self.raftLog:snapshot()
		if snapshot.metadata.index == 0 then
			log.panic('need non-empty snapshot')
		end
		m.snapshot = snapshot
		local sindex, sterm = snapshot.metadata.index, snapshot.metadata.term
		local str = pr:string()
		log.debug(string.format('%d [firstIndex:%d, commit:%d] send snapshot[index:%d, term:%d] to %d [%s]',
			self.id, self.raftLog:firstIndex(), self.raftLog.committed, sindex, sterm, to, str))
		pr:becomeSnapshot(sindex)
		log.debug(string.format('%d paused sending replication messages to %d [%s]', self.id, to, str))
	else
		m.type = g.msgType.APP
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
				log.panic(self.id .. ' is sending append in unhandled state ' .. pr.state)
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
		type = g.msgType.HEARTBEAT;
		commit = commit;
		context = ctx;
	})
end

function this:bcastAppend()
	self.prs:visit(function(id)
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
				log.panic('refused un-refusable auto-leaving confChange')
			end
			self.pendingConfIndex = self.raftLog:lastIndex()
			log.info('initiating automatic transition out of joint configuration ' .. self.prs:string())
		end
	end
	if #rd.entries > 0 then
		local e = rd.entries[#rd.entries]
		self.raftLog:stableTo(e.index, e.term)
	end
	if rd.snapshot.metadata.index ~= 0 then
		rd.raftLog:stableSnapTo(rd.snapshot.metadata.index)
	end
end

function this:maybeCommit()
	local mci = self.prs:committed()
	return self.raftLog:maybeCommit(mci, self.term)
end

function this:reset(term)
	if self.term ~= term then
		self.term = term
		self.vote = g.none
	end
	self.lead = g.none
	self.electionElapsed = 0
	self.heartbeatElapsed = 0
	self:resetRandomizedElectionTimeout()
	self:abortLeaderTransfer()
	self.prs:resetVotes()
	self.prs:resetProgress(self.id, self.raftLog:lastIndex())
	self.pendingConfIndex = 0
	self.uncommittedSize = 0
	self.readOnly = readOnly.new(self.readOnly.option)
end

function this:appendEntry(...)
	local li = self.raftLog:lastIndex()
	for i, v in ipairs({...}) do
		v.term = self.term
		v.index = li + i
	end
	if not self:increaseUncommittedSize(es) then
		log.debug(self.id .. ' appending new entries to log would exceed uncommitted entry size limit; dropping proposal')
		return false
	end
	li = self.raftLog:append(...)
	self.prs.progress[self.id]:maybeUpdate(li)
	self:maybeCommit()
	return true
end

function this:tickElection()
	self.electionElapsed = self.electionElapsed + 1
	if self:promotable() and self:pastElectionTimeout() then
		self.electionElapsed = 0
		local err = self:step({from = self.id, type = g.msgType.HUP}--[[message]]))
		if err then
			log.debug('error occurred during election ' .. err)
		end
	end
end

function this:tickHeartbeat()
	self.heartbeatElapsed = self.heartbeatElapsed + 1
	self.electionElapsed = self.electionElapsed + 1
	if self.electionElapsed >= self.electionTimeout then
		self.electionElapsed = 0
		if self.checkQuorum then
			local err = self:step({from = self.id, type = g.msgType.CHECK_QUORUM}--[[message]])
			if err then
				log.debug('error occurred during checking sending heartbeat: ' .. err)
			end
		end
		if self.state == stateType.LEADER and self.leadTransferee ~= g.none then
			self:abortLeaderTransfer()
		end
	end
	if self.state ~= stateType.LEADER then
		return
	end
	if self.heartbeatElapsed >= self.heartbeatTimeout then
		self.heartbeatElapsed = 0
		local err = self:step({from = self.id, type = g.msgType.BEAT}--[[message]]))
		if err then
			log.debug('error occurred during checking sending heartbeat: ' .. err)
		end
	end
end

function this:becomeFollower(term, lead)
	self.step = stepFollower
	self:reset(term)
	self.tick = self.tickElection
	self.lead = lead
	self.state = stateType.FOLLOWER
	log.info(self.id .. ' became follower at term ' .. self.term)
end

function this:becomeCandidate()
	if self.state == stateType.LEADER then
		log.panic('invalid transition [leader -> candidate]')
	end
	self.step = stepCandidate
	self:reset(self.term + 1)
	self.tick = self.tickElection
	self.vote = self.id
	self.state = stateType.CANDIDATE
	log.info(self.id .. ' became candidate at term ' .. self.term)
end

function this:becomePreCandidate()
	if self.state == stateType.LEADER then
		log.panic('invalid transition [leader -> pre-candidate]')
	end
	self.step = stepCandidate
	self.prs:resetVotes()
	self.tick = self.tickElection
	self.lead = g.none
	self.state = stateType.PRECANDIDATE
	log.info(self.id .. ' became pre-candidate at term ' .. self.term)
end

function this:becomeLeader()
	if self.state == stateType.FOLLOWER then
		log.panic('invalid transition [follower -> leader]')
	end
	self.step = stepLeader
	self:reset(self.term)
	self.tick = self.tickHeartbeat
	self.lead = self.id
	self.state = stateType.LEADER
	self.prs.progress[self.id]:becomeReplicate()
	self.pendingConfIndex = self.raftLog:lastIndex()
	local emptyEnt = g.newEntry()
	if not self:appendEntry(emptyEnt) then
		log.panic('empty entry was dropped')
	end
	self:reduceUncommittedSize(emptyEnt)
	log.info(self.id .. ' became leader at term ' .. self.term)
end

function this:hup(t--[[campaignType]])
	if self.state == stateType.LEADER then
		log.debug(self.id .. ' ignoring msgHup because already leader')
		return
	end
	if not self:promotable() then
		log.warning(self.id .. ' is unpromotable and cant not campaign')
		return
	end
	local ents, err = self.raftLog:slice(self.raftLog.applied + 1, self.raftLog.committed + 1, g.noLimit)
	if err then
		log.panic('unexpected error getting unapplied entries (' .. err .. ')')
	end
	local n = numOfPendingConf(ents)
	if n ~= 0 and self.raftLog.committed > self.raftLog.applied then
		log.panic(string.format('%d cannot campaign at term %d since there are still %d pending configuration changes to apply',
			self.id. self.term, n))
		return
	end
	log.info(self.id .. ' is starting a new election at term ' .. self.term)
	self:campaign(t)
end

function this:campaign(t--[[campaignType]])
	if not self:promotable() then
		self.warning(self.id .. ' is unpromotable; campaign() should have been called')
	end
	local term, voteMsg
	if t == campaignType.PREELECTION then
		self:becomePreCandidate()
		voteMsg = g.msgType.PREVOTE
		term = self.term + 1
	else
		self:becomeCandidate()
		voteMsg = g.msgType.VOTE
		term = self.term
	end
	local _, _, res = self:poll(self.id, util.voteRespMsgType(voteMsg), true)
	if res == g.voteResult.WON then
		if t == campaignType.PREELECTION then
			self:campaign(campaignType.ELECTION)
		else
			self.becomeLeader()
		end
		return
	end
	local ids, i = {}, 1
	local idMap = self.prs.voters:IDs()
	for id, _ in pairs(idMap) do
		ids[i] = id
		i = i + 1
	end
	table.sort(ids)
	for _, id in ipairs(ids) do
		if id ~= self.id then
			log.info(string.format('%d [logterm: %d, index: %d] sent %s request to %d at term %d',
				self.id, self.raftLog:lastTerm(), self.raftLog:lastIndex(), voteMsg, id, self.term))
			local ctx
			if t == campaignType.TRANSFER then
				ctx = {string.byte(t)}
			end
			self:send({{term = term, to = id, type = voteMsg, index = self.raftLog:lastIndex(), logTerm = self.raftLog:lastTerm(), context = ctx}})
		end
	end
end

function this:poll(id, t--[[messageType]], v)
	if v then
		log.info(string.format('%d received %s from %d at term %d', self.id, t, id, self.term))
	else
		log.info(string.format('%d received %s rejection from %d at term %d', self.id, t, id, self.term))
	end
	self.prs:recordVote(id, v)
	return self.prs:tallyVotes()
end

function this:step(m--[[message]])
	if m.term == 0 then
	elseif m.term > self.term then
		if m.type == g.msgType.VOTE or m.type == g.msgType.PREVOTE then
			local force = g.bytesEqual(m.context, {string.byte(campaignType.TRANSFER, 1, campaignType.TRANSFER)})
			local inLease = self.checkQuorum and self.lead ~= g.none and self.electionElapsed < self.electionTimeout
			if not force and inLease then
				log.info(string.format('%d [logterm: %d, index: %d, vote: %d] ignored %s from %d [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)',
					self.id, self.raftLog:lastTerm(), self.raftLog:lastIndex(), self.vote, m.type, m.from, m.logTerm, m.index, self.index, self.electionTimeout - self.electionElapsed))
				return nil
			end
		end
		if m.type == msgType.PREVOTE then
		elseif m.type == msgType.PREVOTE_RESP and not m.reject then
		else
			log.info(string.format('%d [term: %d] received a %s message with higher term from %d [term: %d]',
				self.id, self.term, m.type, m.from, m.term))
			if m.type == msgType.APP or m.type == msgType.HEARTBEAT or m.type == SNAP then
				self:becomeFollower(m.term, m.from)
			else
				self:becomeFollower(m.term, g.none)
			end
		end
	elseif m.term < self.term then
		if (self.checkQuorum or self.preVote) and (m.type == msgType.HEARTBEAT or m.type == msgType.APP) then
			self:send({to = m.from, type = g.msgType.APP_RESP}--[[message]])
		elseif m.type == g.msgType.PREVOTE then
			log.info(string.format('%d [logterm: %d, index: %d, vote: %d] rejected %s from [logterm: %d, index: %d] at term %d',
				self.id, self.raftLog:lastTerm(), self.raftLog:lastIndex(), self.vote, m.type, m.from, m.logTerm, m.index, self.term))
			self:send({to = m.from, term = self.term, type = g.msgType.VOTE_RESP, reject = true}--[[message]])
		else
			log.info(string.format('%d [term: %d] ignored a %s message with lower term from %d [term: %d]',
				self.id, self.term, m.type, m.from, m.term))
		end
		return nil
	end
	if m.type == g.msgType.HUP then
		if self.preVote then
			self:hup(campaignPreElection)
		else
			self:hup(campaignElection)
		end
	elseif m.type == g.msgType.VOTE or m.msgType == g.msgType.PREVOTE then
		local canVote = self.vote == m.from or (self.vote == g.none and self.read == g.none) or (m.type == g.msgType.PREVOTE and m.term > self.term)
		if canVote and self.raftLog:isUpToDate(m.index, m.logTerm) then
			log.info(string.format('%d [logterm: %d, index: %d, vote: %d] cast %s for %d [logterm: %d, index: %d] at term %d',
				self.id, self.raftLog:lastTerm(), self.raftLog:lastIndex(), self.vote, m.type, m.from, m.logTerm, m.index, self.index))
			self:send({to = m.from, term = m.term, type = util.voteRespMsgType(m.type)})
			if m.type == g.msgType.VOTE then
				self.electionElapsed = 0
				self.vote = m.from
			end
		end
	else
		local err = self:step(m)
		if err then return err end
	end
	return nil
end

function this:responseToReadIndexReq(req--[[message]], readIndex)
	if req.from == g.none or req.from == self.id then
		self.readStates[#self.readStates+1] = {
			index = readIndex;
			requestCtx = req.entries[1].data;
		}
		return nil
	end
	return {
		type = g.msgType.READ_INDEX_RESP;
		to = req.from;
		index = readIndex;
		entries = req.entries;
	}--[[readState]]
end



return this