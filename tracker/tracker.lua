local table = table
local pairs = pairs
local ipairs = ipairs
local next = next
local setmetatable = setmetatable

local log = require('log')
local g = require('global')
local jointConfig = require('joint')
local majorityConfig = require('quorum.majority')
local inflights = require('tracker.inflights')

local config = {
	-- voters jointConfig
	-- autoLeave bool
	-- learners majorityConfig
	-- learnersNext majorityConfig
}
local this = config
this.__index = this

this.new = function()
	local o = {
		voters = jointConfig.new();
	}
	setmetatable(o, this)
	return o
end

function this:string()
	local str = self.voters:string()
	if self.learners ~= nil then
		str = str .. ' ' .. self.learners:string()
	end
	if self.learnersNext ~= nil then
		str = str .. ' ' .. self.learnersNext:print()
	end
	if self.autoLeave then
		str = str .. ' autoLeave'
	end
	return str
end

function this:clone()
	local o = {
		voters = jointConfig.new();
		learners = majorityConfig.new();
		learnersNext = majorityConfig.new();
	}
	for k, _ in pairs(self.voters[1]) do
		o.voters[1][k] = 1
	end
	for k, _ in pairs(self.voters[2]) do
		o.voters[2][k] = 1
	end
	for k, _ in pairs(self.learners) do
		o.learners[k] = 1
	end
	for k, _ in pairs(self.learnersNext) do
		o.learnersNext[k] = 1
	end
	setmetatable(o, this)
	return o
end

local progressTracker = {
	-- config config
	-- progress table[num]progress
	-- votes table[num]bool
	-- maxInflight num
}
local this = progressTracker
this.__index = this

this.new = function(maxInflight)
	local o = {
		maxInflight = maxInflight;
		config = config.new();
		votes = {};
		progress = {};
	}
	setmetatable(o, this)
	return o
end

function this:confState()
	local t = g.newConfState()
	t.voters = self.config.voters[1]:slice();
	t.votersOutgoing = self.config.voters[2]:slice();
	t.learners = self.config.learners:slice();
	t.learnersNext = self.config.learnersNext:slice();
	t.autoLeave = self.config.autoLeave;
	return t
end

function this:isSingleton()
	return self.config.voters[1]:size() == 1 and self.config.voters[2]:size() == 0
end

function this:committed()
	return self.config.voters:committedIndex(self.progress)
end

function this:visit(f)
	local t, i = {}, 1
	for id, _ in pairs(self.progress) do
		t[i] = id
		i = i + 1
	end
	table.sort(t)
	for _, id in ipairs(t) do
		f(id, self.progress[id])
	end
end

function this:quorumActive()
	local votes = {}
	self:visit(function(id, pr)
		if pr.isLearner then
			return
		end
		votes[id] = pr.recentActive
	end)
	return self.config.voters:voteResult(votes) == g.voteResult.WON
end

function this:voterNodes()
	local m = self.config.voters.IDs()
	local nodes, i = {}, 1
	for id, _ in pairs(m) do
		nodes[i] = id
		i = i + 1
	end
	table.sort(nodes)
	return nodes
end

function this:learnerNodes()
	if not next(self.config.learners) then
		return nil
	end
	local nodes, i = {}, 1
	for id, _ in pairs(self.config.learners) do
		nodes[i] = id
		i = i + 1
	end
	table.sort(nodes)
	return nodes
end

function this:resetVotes()
	self.votes = {}
end

function this:resetProgress(id, lastIndex)
	for _, v in pairs(self.progress) do
		v.next = lastIndex + 1
		v.inflights = inflights.new(self.maxInflight)
		if v.id == id then
			v.match = lastIndex
		else
			v.match = 0
		end
	end
end

function this:recordVote(id, v)
	if not self.votes[id] then
		self.votes[id] = v
	end
end

function this:tallyVotes()
	local granted, rejected = 0, 0
	for id, pr in pairs(self.progress) do
		if not pr.isLearner and self.votes[id] ~= nil then
			if self.votes[id] then
				granted = granted + 1
			else
				rejected = rejected + 1
			end
		end
	end
	return granted, rejected, self.config.voters:voteResult(self.votes)
end

return this