local ipairs = ipairs
local unpack = unpack

local g = require('global')

local restore = {}
local this = restore

this.toConfChangeSingle = function(cs)
	local out, in, i, j = {}, {}, 1, 1
	for _, id in ipairs(cs.votersOutgoing) do
		local s = g.newConfChangeSingle()
		s.type = g.confChangeType.ADD_NODE
		s.nodeID = id
		out[i] = s
		i = i + 1
	end
	for _, id in ipairs(cs.votersOutgoing) do
		local s = g.newConfChangeSingle()
		s.type = g.confChangeType.REMOVE_NODE
		s.nodeID = id
		in[j] = s
		j = j + 1
	end
	for _, id in ipairs(cs.voters) do
		local s = g.newConfChangeSingle()
		s.type = g.confChangeType.ADD_NODE
		s.nodeID = id
		in[j] = s
		j = j + 1
	end
	for _, id in ipairs(cs.learners) do
		local s = g.newConfChangeSingle()
		s.type = g.confChangeType.ADD_LEARNERNODE
		s.nodeID = id
		in[j] = s
		j = j + 1
	end
	for _, id in ipairs(cs.learnersNext) do
		local s = g.newConfChangeSingle()
		s.type = g.confChangeType.ADD_LEARNERNODE
		s.nodeID = id
		in[j] = s
		j = j + 1
	end
	return out, in
end

this.chain = function(chg, ops)
	for _, op in ipairs(ops) do
		local cfg, prs, err = op(chg)
		if err then
			return {}, nil, err
		end
		chg.tracker.config = cfg
		chg.tracker.progress = prs
	end
	return chg.tracker.config, chg.tracker.progress, nil
end

this.restore = function(chg, cs)
	local outgoing, incoming = toConfChangeSingle(cs)
	local ops, i = {}, 1
	if #outgoing == 0 then
		for _, cc in ipairs(incoming) do
			ops[i] = function(chg) return chg:simple(cc) end
			i = i + 1
		end
	else
		for _, cc in ipairs(outgoing) do
			ops[i] = function(chg) return chg:simple(cc) end
			i = i + 1
		end
		ops[i] = function(chg) return chg:enterJoint(cs.autoLeave, unpack(incoming)) end
	end
	return this.chain(chg, unpack(ops))
end

return this