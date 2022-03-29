local setmetatable = setmetatable
local table = table
local string = string
local ipairs = ipairs

local g = require('global')
local log = require('log')

local unstable = {
	-- snapshot snapshot
	-- entries table[num]entry
	-- offset num
}
local this = unstable
this.__index = this

this.new = function()
	local o = {
		snapshot = g.newSnapshot();
		entries = {};
		offset = 0;
	}
	setmetatable(o, this)
	return o
end

function this:maybeFirstIndex()
	if self.snapshot then
		return self.snapshot.metadata.index + 1, true
	end
	return 0, false
end

function this:maybeLastIndex()
	if #self.entries > 0 then
		return self.offset + #self.entries - 1, true
	end
	if self.snapshot then
		return self.snapshot.metadata.index, true
	end
	return 0, false
end

function this:maybeTerm(i)
	if i < self.offset then
		if self.snapshot and self.snapshot.metadata.index == i then
			return self.snapshot.metadata.term, true
		end
		return 0, false
	end
	local last, ok = self:maybeLastIndex()
	if not ok or i > last then
		return 0, false
	end
	return self.entries[i-self.offset].term, true
end

function this:stableTo(i, t)
	local gt, ok = self:maybeTerm(i)
	if not ok then
		return
	end
	if gt == t and i >= self.offset then
		self.entries = table.sub(self.entries, i-self.offset+1)
		self.offset = i + 1
	end
end

function this:stableSnapTo(i)
	if self.snapshot and self.snapshot.metadata.index == i then
		self.snapshot = nil
	end
end

function this:restore(s)
	self.offset = s.metadata.index + 1
	self.entries = nil
	self.snapshot = s
end

function this:truncateAndAppend(ents)
	local after = ents[1].index
	if after == self.offset + #self.entries then
		local i = #self.entries + 1
		for _, v in ipairs(ents) do
			self.entries[i] = v
			i = i + 1
		end
	elseif after <= self.offset then
		log.print('replace the unstable entries from index ' .. after)
		self.offset = after
		self.entries = ents
	else
		log.print('truncate the unstable entries before index ' .. after)
		self.entries = self:slice(lo, hi)
		local i = #self.entries + 1
		for _, v in ipairs(ents) do
			self.entries[i] = v
			i = i + 1
		end
	end
end

function this:slice(lo, hi)
	self:mustCheckOutOfBounds(lo, hi)
	return table.sub(self.entries, lo-self.offset, hi-self.offset)
end

function this:mustCheckOutOfBounds(lo, hi)
	if lo > hi then
		g.panic('invalid unstable.slice ' .. lo .. ' > ' .. hi)
	end
	local upper = self.offset + #self.entries
	if lo < self.offset or hi > upper then
		g.panic(string.format('unstable.slice[%d,%d) out of bound [%d,%d]', lo, hi, self.offset, upper))
	end
end

return this