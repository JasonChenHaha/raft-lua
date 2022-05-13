local ipairs = ipairs
local setmetatable = setmetatable

local log = require('log')

local inflights = {
	-- start num;
	-- count num;
	-- size num;
	-- buffer table[num]num;
}
local this = inflights
this.__index = this

this.new = function(size)
	local o = {
		start = 0;
		count = 0;
		size = size;
		buffer = {};
	}
	setmetatable(o, this)
	return o
end

function this:clone()
	local o = this.new(self.size)
	for k, v in ipairs(self.buffer) do
		o.buffer[k] = v
	end
	return o
end

function this:add(inflight)
	if self:full() then
		log.panic('cannot add into a full inflights')
	end
	local next = self.start + self.count
	if next > self.size then
		next = next - self.size
	end
	self.buffer[next] = inflight
	self.count = self.count + 1
end

function this:freeLE(to)
	if self.count == 0 or to < self.buffer[self.start] then
		return
	end
	local idx, i = self.start, 0
	repeat
		if to < self.buffer[idx] then
			break
		end
		idx = idx + 1
		if idx > self.size then
			idx = idx - self.size
		end
		i = i + 1
	until i >= self.count
	self.count = self.count - i
	if self.count == 0 then
		self.start = 0
	end
end

function this:freeFirstOne()
	self:freeLE(self.buffer[self.start])
end

function this:full()
	return self.count == self.size
end

function this:count()
	return self.count
end

function this:reset()
	self.count = 0
	self.start = 0
end

return this