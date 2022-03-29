local setmetatable = setmetatable
local string = string

local cjson	= require('cjson.safe')

local log = require('log')

local global = {}
local this = global

this.none = 0
this.noLimit = math.huge

this.errCode = {
	COMPACTED = 1;
	SNAP_OUT_OF_DATE = 2;
	UNAVAILABLE = 3;
	SNAPSHOT_TEMPORARILY_UNAVAILABLE = 4;
	STEP_LOCAL_MSG = 5;
	STEP_PEER_NOT_FOUND = 6;
	PROPOSAL_DROPPED = 7;
}
this.errText = {
	[1] = 'requested index is unavailable due to compaction';
	[2] = 'requested index is older than the existing snapshot';
	[3] = 'requested entry at index is unavailable';
	[4] = 'snapshot is temporarily unavailable';
	[5] = 'raft: cannot step raft local message';
	[6] = 'raft: cannot step as peer not found';
	[7] = 'raft proposal dropped';
}


this.entryType = {
	NORMAL = 0;
	CONFCHANGE = 1;
}

this.messageType = {
	HUP = 0;
	BEAT = 1;
	PROP = 2;
	APP = 3;
	APPRESP = 4;
	VOTE = 5;
	VOTE_RESP = 6;
	SNAP = 7;
	HEARTBEAT = 8;
	HEARTBEAT_RESP = 9;
	UNREACHABLE = 10;
	SNAP_STATUS = 11;
	CHECK_QUORUM = 12;
	TRANSFER_LEADER = 13;
	TIMEOUT_NOW = 14;
	READ_INDEX = 15;
	READ_INDEX_RESP = 16;
	PREVOTE = 17;
	PREVOTE_RESP = 18;
}

this.confChangeTransition = {
	AUTO = 0;
	JOINT_IMPLICIT = 1;
	JOINT_EXPLICIT = 2;
}

this.confChangeType = {
	ADD_NODE = 0;
	REMOVE_NODE = 1;
	UPDATE_NODE = 2;
	ADD_LEARNERNODE = 3;
}

this.stateType = {
	PROBE = 0;
	REPLICATE = 1;
	SNAPSHOT = 2;
}

this.stateTypeString = {
	[0] = 'probe';
	[1] = 'replicate';
	[2] = 'snapshot';
}

this.voteResult = {
	PENDING = 0;
	LOST = 1;
	WON = 2;
}

this.snapshotStatus = {
	FINISH = 1;
	FAILURE = 2;
}

this.readState = {
	-- index num
	-- requestCtx table[]num
}

local readOnlyOption = {
	SAFE = 0;
	LEASE_BASED = 1;
}

this.panic = function(err)
	if type(err) == 'number' then
		err = this.errText[err]
	end
	log.print(err)
	ngx.exit(0)
end

local entry = {
	-- term num				任期号
	-- index num			索引号
	-- type entryType		normal类型或者confchange类型
	-- data table[]num		具体数据
}
entry.__index = entry
function entry:size()
	return 24 + #self.data * 8
end
this.newEntry = function()
	local o = {}
	setmetatable(o, entry)
	return o
end

local softState = {
	-- lead num
	-- raftState stateType
}
softState.__index = softState
function softState:equal(s)
	return self.lead == s.lead and self.raftState == s.raftState
end

this.newSnapshotMetadata = function()
	return {
		-- confState confState
		-- index num
		-- term num
	}
end

this.newSnapshot = function()
	return {
		-- data table[num]num
		-- metadata snapshotMetadata
	}
end

this.newMessage = function()
	return {
		-- type messageType
		-- to num
		-- from num
		-- term num
		-- logTerm num
		-- index num
		-- entries table[num]entry
		-- commit num
		-- snapshot snapShot
		-- reject bool
		-- rejectHint num
		-- context table[num]num
	}
end

this.newConfState = function()
	return {
		-- voters table[num]num
		-- votersOutgoing table[num]num
		-- learners table[num]num
		-- learnersNext table[num]num
		-- autoLeave bool
	}
end

this.newHardState = function()
	return {
		-- term num
		-- vote num
		-- commit num
	}
end

this.newConfChange = function()
	return {
		-- transition confChangeTransition
		-- changes table[num]confChangeSingle
		-- context table[num]num
	}
end

this.newConfChangeSingle = function()
	return {
		-- type confChangeType
		-- nodeID num
	}
end

string.concat = function(char, num)
	local t = {}
	for i = 1, num do
		t[i] = char
	end
	return table.concat(t)
end

table.toJson = function(tab)
	return cjson.encode(tab)
end

table.sub = function(tab, l, r)
	l = l or 1
	r = r or #tab
	if l > #tab or r < l then
		return nil
	end
	local t, x = {}, 1
	for i = l, r do
		t[x] = tab[i]
		x = x + 1
	end
	return t
end

this.limitSize = function(ents, maxSize)
	if #ents == 0 then
		return
	end
	local size = ents[1]:size()
	local limit = 1
	while limit < #ents do
		size = size + ents[limit]:size()
		if size > maxSize then
			break
		end
		limit = limit + 1
	end
	for i = limit, #ents do
		ents[i] = nil
	end
end

this.table_compare = function(t1, t2)
	local function base_compare(t1, t2)
		if not t2 then return false end
		for k, v in pairs(t1) do
			if type(v) == 'table' then
				if not base_compare(t1[k], t2[k]) then
					return false
				end
			elseif v ~= t2[k] then
				return false
			end
		end
		return true
	end
	return base_compare(t1, t2) and base_compare(t2, t1)
end

return this