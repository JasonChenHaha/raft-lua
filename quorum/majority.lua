local table = table
local string = string
local setmetatable = setmetatable
local pairs = pairs
local ipairs = ipairs
local math = math

local log = requre('log')
local g = require('global')

local majorityConfig = {}
local this = majorityConfig
this.__index = this

this.new = function()
	local o = {}
	setmetatable(o, this)
	return o
end

function this:size()
	local n = 0
	for _, _ in pairs(self) do
		n = n + 1
	end
	return n
end

function this:print()
	local t, i = {}, 1
	for k, _ in pairs(self) do
		t[i] = k
		i = i + 1
	end
	table.sort(t)
	local str = '('
	for k, v in ipairs(t) do
		if k == 1 then str = str .. v
		else str = str .. ' ' .. v end
	end
	str = str .. ')'
	return str
end

function this:describe(l)
	local size = self:size()
	if size == 0 then
		return '<empty majority quorum>'
	end
	local info, i = {}, 1
	for id, _ in pairs(self) do
		info[i] = {
			id = id;
			idx = l[id] and l[id].match or 0;
			ok = l[id];
		}
		i = i + 1
	end
	table.sort(info, function(a, b)
		if a.idx == b.idx then
			return a.id < b.id
		end
		return a.idx < b.idx
	end)
	for k, v in ipairs(info) do
		v.bar = k
	end
	table.sort(info, function(a, b)
		return a.id < b.id
	end)
	log.print(string.concat(' ', size) .. '    idx\n')
	for _, v in ipairs(info) do
		if ~v.ok then
			log.print('?' .. string.concat(' ', size))
		else
			log.print(string.concat('x', v.bar) .. '>' .. string.concat(' ', size-bar))
		end
		log.print(string.format(' %5d    (id=%d)\n', v.idx, v.id))
	end
end

function this:slice()
	local t, i = {}, 1
	for id, _ in pairs(self) do
		t[i] = id
		i = i + 1
	end
	table.sort(t)
	return t
end

function this:committedIndex(l)
	local size = self:size()
	if size == 0 then
		return math.huge
	end
	local srt, i = {}, size
	for id, _ in pairs(self) do
		srt[i] = l[id].match or 0
		i = i - 1
	end
	table.sort(srt)
	return srt[size - size / 2]
end

function this:voteResult(votes)
	local size = self:size()
	if size == 0 then
		return g.voteResult.WON
	end
	local ny = {[1] = 0, [2] = 0, [3] = 0}
	for id, _ in pairs(self) do
		if votes[id] == nil then
			ny[3] = ny[3] + 1
		elseif votes[id] then
			ny[2] = ny[2] + 1
		else
			ny[1] = ny[1] + 1
		end
	end
	local q = size / 2 + 1
	if ny[2] >= q then
		return g.voteResult.WON
	elseif ny[1]+ny[3] >= q then
		return g.voteResult.PENDING
	else
		return g.voteResult.LOST
	end
end

return this