local ipairs = ipairs
local table = table
local setmetatable = setmetatable

local readIndexStatus = {
	-- req message
	-- index num
	-- acks table[num]bool
}

local readOnly = {
	-- option readOnlyOption
	-- pendingReadIndex table[string]readIndexStatus
	-- readIndexQueue table[]string
}
local this = readOnly
this.__index = this

this.new = function(option)
	local o = {
		option = option;
		pendingReadIndex = {};
	}
	setmetatable(o, this)
	return o
end

function this:addRequest(index, m)
	local s = m.entries[1].data
	if self.pendingReadIndex[s] then
		return
	end
	self.pendingReadIndex[s] = {
		index = index;
		req = m;
		acks = {};
	}
	self.readIndexQueue[#self.readIndexQueue + 1] = s
end

function this:recvAck(id, context)
	local rs = self.pendingReadIndex[context]
	if rs then
		rs.acks[id] = true
		return rs.acks
	end
end

function this:advance(m)
	local i, j, found, rs = 2, 1
	local ctx = m.context
	local rss = {}

	for _, okctx in ipairs(self.readIndexQueue) do
		i = i + 1
		rs = self.pendingReadIndex[okctx]
		if not rs then
			log.panic('cannot find corresponding read state from pending map')
		end
		rss[j] = rs
		j = j + 1
		if okctx == ctx then
			found = true
			break
		end
	end
	if found then
		self.readIndexQueue = table.sub(self.readIndexQueue, i)
		for _, rs in ipairs(rss) do
			self.pendingReadIndex[rs.req.entries[1].data] = nil
		end
		return rss
	end
end

function this:lastPendingRequestCtx()
	if #self.readIndexQueue == 0 then
		return ''
	end
	return self.readIndexQueue[#self.readIndexQueue]
end

return this