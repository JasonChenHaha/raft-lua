local table = table
local setmetatable = setmetatable

local g = require('global')
local log = require('log')

local storage = {
	-- hardState hardState
	-- snapshot snapshot
	-- ents table[num]entry
}
local this = storage
this.__index = this

this.new = function()
	local o = {
		ents = {}
	}
	setmetatable(o, this)
	return o
end

function this:initialState()
	return self.hardState, self.snapshot.metadata.confState
end

function this:setHardState(st)
	-- fix me
	-- lock
	self.hardState = st
end

function this:entries(lo, hi, maxSize)
	-- fix me
	-- lock
	local offset = self.ents[1].index
	if lo <= offset then
		return nil, g.errCode.COMPACTED
	end
	if hi > self:lastIndex() + 1 then
		log.panic('entries hi(' .. hi .. ') is out of bound lastindex(' .. self:lastIndex() .. ')')
	end
	if #self.ents == 1 then
		return nil, g.text.unavailable
	end
	local ents = table.sub(self.ents, lo - offset + 1, hi - offset + 1)
	return limitSize(ents, maxSize)
end

function this:term(i)
	-- fix me
	-- lock
	local offset = self.ents[1].index
	if i < offset then
		return 0, g.errCode.COMPACTED
	end
	if i - offset >= #self.ents then
		return 0, g.text.unavailable
	end
	return self.ents[i - offset].term
end

function this:lastIndex()
	-- fix me
	-- lock
	return self.ents[1].index + #self.ents - 1
end

function this:firstIndex()
	-- fix me
	-- lock
	return self.ents[1].index + 1
end

function this:snapshot()
	-- fix me
	-- lock
	return self.snapshot
end

function this:applySnapshot(snap)
	-- fix me
	-- lock
	local index = self.snapshot.metadata.index
	local snapIndex = snap.metadata.index
	if index >= snapIndex then
		return g.errCode.SNAP_OUT_OF_DATE
	end
	self.snapshot = snap
	self.ents = {term = snap.metadata.term, index = snap.metadata.index}
end

function this:createSnapShot(i, cs, data)
	-- fix me
	-- lock
	if i < self.snapshot.metadata.index then
		return g.newSnapshot(), g.errCode.SNAP_OUT_OF_DATE
	end
	local offset = self.ents[1].index
	if i > self:lastIndex() then
		log.panic('snapshot ' .. i .. ' is out of bound lastindex(' .. self:lastIndex() .. ')')
	end
	self.snapshot.metadata.index = i
	self.snapshot.metadata.term = self.ents[i - offset].term
	if cs then
		self.snapshot.metadata.confState = cs
	end
	self.snapshot.data = data
	return self.snapshot
end

function this:compact(compactIndex)
	-- fix me
	-- lock
	local offset = self.ents[1].index
	if compactIndex <= offset then
		return g.errCode.COMPACTED
	end
	if compactIndex > self:lastIndex() then
		log.panic('snapshot ' .. i .. ' is out of bound lastindex(' .. self:lastIndex() .. ')')
	end
	local i = compactIndex - offset
	local ents, k = {g.newEntry()}, 2
	ents[1].index = self.ents[i].index
	ents[1].term = self.ents[i].term
	for j = i + 1, #self.ents do
		ents[k] = self.ents[j]
		k = k + 1
	end
	self.ents = ents
end

function this:append(entries)
	if #entries == 0 then
		return
	end
	-- fix me
	-- lock

	local first = self:firstIndex()
	local last = entries[1].index + #entries - 1

	if last < first then
		return
	end

	if first > entries[1].index then
		entries = table.sub(entries, first - entries[1].index + 1, #entries)
	end

	local offset = entries[1].index - self.ents[1].index
	if #self.ents > offset then
		local ents, j = {}, offset + 1
		for i = 1, offset do
			ents[i] = self.ents[i]
		end
		for i = 1, #entries do
			ents[j] = entries[i]
			j = j + 1
		end
		self.ents = ents
	elseif #self.ents == offset then
		local j = offset + 1
		for i = 1, #entries do
			self.ents[j] = entries[i]
			j = j + 1
		end
	else
		log.panic('missing log entry [last:' .. self:lastIndex() .. ', append at:' .. entries[1].index .. ']')
	end
end

return this