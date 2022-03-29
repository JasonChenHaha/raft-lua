local math = math
local string = string
local setmetatable = setmetatable

local g = require('global')
local log = require('log')

local progress = {
	-- match num;
	-- next num;
	-- state stateType;
	-- pendingSnapshot num;
	-- recentActive bool;
	-- probeSent bool;
	-- inflights inflights;
	-- isLearner bool;
}
local this = progress
this.__index = this

this.new = function()
	local o = {}
	setmetatable(o, this)
	return o
end

function this:resetState(state)
	self.probeSent = false
	self.pendingSnapshot = 0
	self.state = state
	self.inflights:reset()
end

function this:probeAcked()
	self.probeSent = false
end

function this:becomeProbe()
	self.resetState(g.stateType.PROBE)
	if self.state == g.stateType.SNAPSHOT then
		self.next = math.max(self.match+1, self.pendingSnapshot+1)
	else
		self.next = self.match + 1
	end
end

function this:becomeReplicate()
	self.resetState(g.stateType.REPLICATE)
	self.next = self.match + 1
end

function this:becomeSnapshot(snapshot)
	self.resetState(g.stateType.SNAPSHOT)
	self.pendingSnapshot = snapshot
end

function this:maybeUpdate(n)
	local updated
	if self.match < n then
		self.match = n
		updated = true
		self:probeAcked()
	end
	self.next = math.max(self.next, n+1)
	return updated
end

function this:optimisticUpdate(n)
	self.next = n + 1
end

function this:maybeDecrTo(rejected, matchHint)
	if self.state == g.stateType.REPLICATE then
		if rejected <= self.match then
			return false
		end
		self.next = self.match + 1
		return true
	end
	if self.next-1 ~= rejected then
		return false
	end
	self.next = math.max(math.min(rejected, matchHint+1), 1)
	self.probeSent = false
	return true
end

function this:isPaused()
	if self.state == g.stateType.PROBE then
		return self.probeSent
	elseif self.state == g.stateType.REPLICATE then
		return self.inflights:full()
	elseif self.state == g.stateType.SNAPSHOT then
		return true
	else
		g.panic('unexpected state')
	end
end

function this:string()
	local str = string.format('%s match=%s next=%d;', self.state, self.match, self.next)
	if self.isLearner then
		str = str .. 'learner;'
	end
	if self.isPaused() then
		str = str .. 'paused;'
	end
	if self.pendingSnapshot > 0 then
		str = str .. 'pendingSnap=' .. self.pendingSnapshot .. ';'
	end
	if ~self.recentActive then
		str = str .. 'inactive;'
	end
	local n = self.inflights:count()
	if n > 0 then
		str = str .. 'inflight=' .. n .. (self.inflights:full() and '[full]' or '')
	end
	return str
end

return this