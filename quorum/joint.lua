local setmetatable = setmetatable
local ipairs = ipairs
local pairs = pairs

local majorityConfig = require('quorum.majority')

local jointConfig = {
	-- [1] = majorityConfig
	-- [2] = majorityConfig
}
local this = jointConfig
this.__index = this

this.new = function()
	local o = {
		[1] = majorityConfig.new();
		[2] = majorityConfig.new();
	}
	setmetatable(o, this)
	return o
end

function this:string()
	local str = self[1]:string()
	if self[2]:size() > 0 then
		str = str .. '&&' .. self[2]:string()
	end
	return str
end

function this:IDs()
	local mc = majorityConfig.new()
	for _, cell in ipairs(self) do
		for id, _ in pairs(cell) do
			mc[id] = 1
		end
	end
	return mc
end

function this:describe(l)
	self.IDs():describe(l)
end

function this:committedIndex(l)
	local idx0 = self[1]:committedIndex(l)
	local idx1 = self[2]:committedIndex(l)
	return idx0 < idx1 and idx0 or idx1
end

function this:voteResult(votes)
	local r1 = self[1]:voteResult(votes)
	local r2 = self[2]:voteResult(votes)
	if r1 == r2 then
		return r1
	end
	if r1 == g.voteResult.LOST or r2 == g.voteResult.LOST then
		return g.voteResult.LOST
	end
	return g.voteResult.PENDING
end

return this