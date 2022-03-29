local pairs = pairs
local ipairs = ipairs
local table = table

local g = require('global')
local majorityConfig = require('quorum.majority')
local progress = require('tracker.progress')
local inflights = require('tracker.inflights')

local changer = {
	-- tracker progressTracker
	-- lastIndex num
}
local this = changer
this.__index = this

local function checkInvariants(cfg, prs)
	local mc = cfg.voters:IDs()
	for id, _ in pairs(mc) do
		if not prs[id] then
			return 'no progress for ' .. id
		end
	end
	for id, _ in pairs(cfg.learners) do
		if not prs[id] then
			return 'no progress for ' .. id
		end
		if cfg.voters[2][id] then
			return id .. ' is in learners and voters[2]'
		end
		if cfg.voters[1][id] then
			return id .. ' is in learners and voters[1]'
		end
		if not prs[id].isLearner then
			return id .. ' is in learners, but is not marked as learner'
		end
	end
	for id, _ in pairs(cfg.learnersNext) do
		if not prs[id] then
			return 'no progress for ' .. id
		end
		if not cfg.voters[2][id] then
			return id .. ' is in learnersNext, but not voters[2]'
		end
		if prs[id].isLearner then
			return id .. ' is in learnersNext, but is already marked as learner'
		end
	end
	if cfg.voters[2]:size() == 0 then
		if cfg.learnersNext then
			return 'cfg.learnersNext must be nil when not joint'
		end
		if cfg.autoLeave then
			return 'autoLeave must be false when not joint'
		end
	end
	return nil
end

local function checkAndReturn(cfg, prs)
	local err = checkInvariants(cfg, prs)
	if err then
		return {}, {}, err
	end
	return cfg, prs, nil
end

function this:enterJoint(autoLeave, ...)
	local cfg, prs, err = self:checkAndCopy()
	if err then
		return err
	end
	if cfg.voters[2]:size() > 0 then
		return 'config is already joint'
	end
	if cfg.voters[1]:size() == 0 then
		return "can't make a zero-voter config joint"
	end
	cfg.voters[2] = majorityConfig.new()
	for id, _ in pairs(cfg.voters[1]) do
		cfg.voters[2][id] = 1
	end
	err = self:apply(cfg, prs, ...)
	if err then
		return err
	end
	cfg.autoLeave = autoLeave
	return self:checkAndReturn(cfg, prs)
end

function this:leaveJoint()
	local cfg, prs, err = self:checkAndCopy()
	if err then
		return err
	end
	if cfg.voters[2]:size() == 0 then
		return 'configuration is not joint ' .. table.toJson(cfg)
	end
	for id, _ in pairs(cfg.learnersNext) do
		cfg.learners[id] = 1
		prs[id].isLearner = true
	end
	cfg.learnersNext = nil
	for id, _ in pairs(cfg.voters[2]) do
		if not cfg.voters[1][id] and not cfg.learners[id] then
			prs[id] = nil
		end
	end
	cfg.voters[2] = nil
	cfg.autoLeave = false
	return checkAndReturn(cfg, prs)
end

function this:simple(...)
	local cfg, prs, err = this:checkAndCopy()
	if err then
		return {}, nil, err
	end
	if cfg.voters[2]:size() > 0 then
		return {}, nil, "can't apply simple config change in joint config"
	end
	err = self:apply(cfg, prs, ...)
	if err then
		return err
	end
	for i, mc in pairs(self.tracker.config.voters) do
		for id, _ in pairs(mc) do
			if not cfg.voters[i][id] then
				return {}, nil, 'more than one voter changed without entering joint config'
			end
		end
	end
	for i, mc in pairs(cfg.voters) do
		for id, _ in pairs(mc) do
			if not self.tracker.config.voters[i][id] then
				return {}, nil, 'more than one voter changed without entering joint config'
			end
		end
	end
	return checkAndReturn(cfg, prs)
end

function this:apply(cfg, prs, ...)
	for _, cc in ipairs({...}) do
		if cc.nodeID ~= 0 then
			if cc.type == g.confChangeType.ADD_NODE then
				self:makeVoter(cfg, prs, cc.nodeID)
			elseif cc.type == g.confChangeType.ADD_LEARNERNODE then
				self:makeLearner(cfg, prs, cc.nodeID)
			elseif cc.type == g.confChangeType.REMOVE_NODE then
				self:remove(cfg, prs, cc.nodeID)
			elseif cc.type == g.confChangeType.UPDATE_NODE then
			else
				return 'unexpected conf type ' .. cc.type
			end
		end
	end
	if cfg.voters[1].size == 0 then
		return 'removed all voters'
	end
	return nil
end

function this:makeVoter(cfg, prs, id)
	if not prs[id] then
		self:initProgress(cfg, prs, id, false)
		return
	end
	prs[id].isLearner = false
	cfg.learners[id] = nil
	cfg.learnersNext[id] = nil
	cfg.voters[1][id] = 1
end

function this:makeLearner(cfg, prs, id)
	if not prs[id] then
		self:initProgress(cfg, prs, id, true)
		return
	end
	if prs[id].isLearner then
		return
	end
	self:remove(cfg, prs, id)
	prs[id] = pr
	if cfg.voters[2][id] then
		cfg.learnersNext[id] = 1
	else
		prs[id].isLearner = true
		cfg.learners[id] = 1
	end
end

function this:remove(cfg, prs, id)
	if not prs[id] then
		return
	end
	cfg.voters[1][id] = nil
	cfg.learners[id] = nil
	cfg.learnersNext[id] = nil
	if cfg.voters[2][id] then
		prs[id] = nil
	end
end

function this:initProgress(cfg, prs, id, isLearner)
	if not isLearner then
		cfg.voters[1][id] = 1
	else
		cfg.learners[id] = 1
	end
	local pr = progress.new()
	pr.next = self.lastIndex
	pr.match = 0
	pr.inflights = inflights.new(self.tracker.maxInflights)
	pr.isLearner = isLearner
	recentActive = true
	prs[id] = pr
end

function this:checkAndCopy()
	local cfg = self.tracker.config:clone()
	local prs = {}
	for id, pr in pairs(self.tracker.progress) do
		prs[id] = pr
	end
	return checkAndReturn(cfg, prs)
end

return this