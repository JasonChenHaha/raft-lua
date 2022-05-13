local string = string
local math = math
local unpack = unpack
local setmetatable = setmetatable
local ngx = ngx

local log = require('log')
local unstable = require('log_unstable')

local raftLog = {
	-- storage storage
	-- unstable unstable
	-- committed num
	-- applied num
	-- maxNextEntsSize num
}
local this = raftLog
this.__index = this

this.newLog = function(storage, maxNextEntsSize)
	if not storage then
		log.panic('storage must not be nil')
	end
	local o = {
		storage = storage;
		unstable = unstable.new();
		maxNextEntsSize = maxNextEntsSize or g.noLimit;
	}
	local firstIndex, err = storage:firstIndex()
	if err then
		log.panic(err)
	end
	local lastIndex, err = storage:lastIndex()
	if err then
		log.panic(err)
	end
	o.unstable.offset = lastIndex + 1
	o.committed = firstIndex - 1
	o.applied = firstIndex - 1
	setmetatable(o, this)
	return o
end

function this:string()
	return string.format("committed=%d, applied=%d, unstsable.offset=%d, len(unstable.entries)=%d", self.committed, self.applied, self.unstable.offset, #self.unstable.entries)
end

function this:maybeAppend(index, logTerm, committed, ...)
	local ents = {...}
	if self:matchTerm(index, logTerm) then
		local lastnewi = index + #ents
		local ci = self:findConflict(ents)
		if ci == 0 or ci <= self.committed then
			log.panic('entry ' .. ci .. ' conflict with committed entry [committed(' .. self.committed .. ')]')
		else
			local offset = index + 1
			self:append(unpack(table.sub(ents, ci - offset + 1, #ents)))
		end
		self:commitTo(math.min(committed, lastnewi))
		return lastnewi, true
	end
	return 0, false
end

function this:append(...)
	local ents = {...}
	if #ents == 0 then
		return self:lastIndex()
	end
	local after = ents[1].index - 1
	if after < self.committed then
		log.panic('after (' .. after .. ') is out of range [committed(' .. self.committed .. ')]')
	end
	self.unstable:truncateAndAppend(ents)
	return self:lastIndex()
end

function this:findConflict(ents)
	for _, v in ipairs(ents) do
		if not self:matchTerm(v.index, v.term) then
			if v.index < self:lastIndex() then
				log.info(string.format('found conflict at index %d [existing term: %d, conflicting term: %d]',
					v.index, self:zeroTermOnErrCompacted(self:term(v.index)), v.term))
			end
			return v.index
		end
	end
	return 0
end

function this:findConflictByTerm(index, term)
	local li = self:lastIndex()
	if index > li then
		log.warning('index(' .. index .. ') is out of range [0, lastIndex(' .. li .. ')] in findConflictByTerm')
		return index
	end
	while true do
		local logTerm, err = self:term(index)
		if err or logTerm <= term then
			break
		end
		index = index - 1
	end
	return index
end

function this:unstableEntries()
	if #self.unstable.entries == 0 then
		return nil
	end
	return self.unstable.entries
end

function this:nextEnts()
	local off = math.max(self.applied + 1, self:firstIndex())
	if self.committed + 1 > off then
		local ents, err = self:slice(off, self.committed + 1, self.maxNextEntsSize)
		if err then
			log.panic('unexpected error when getting unapplied entries (' .. err .. ')')
		end
		return ents
	end
	return nil
end

function this:hasNextEnts()
	return self.committed + 1 > math.max(self.applied + 1, self:firstIndex())
end

function this:hasPendingSnapshot()
	return self.unstable.snapshot ~= nil and self.unstable.snapshot.metadata.index ~= 0
end

function this:snapshot()
	if self.unstable.snapshot ~= nil then
		return self.unstable.snapshot
	end
	return self.storage:snapshot()
end

function this:firstIndex()
	local i, ok = self.unstable:maybyFirstIndex()
	if ok then
		return i
	end
	local i, err = self.storage:firstIndex()
	if err then
		log.panic(err)
	end
	return i
end

function this:lastIndex()
	local i, ok = self.unstable.maybeLastIndex()
	if ok then
		return i
	end
	local i, err = self.storage.lastIndex()
	if err then
		log.panic(err)
	end
	return i
end

function this:commitTo(tocommit)
	if self.committed < tocommit then
		if self:lastIndex() < tocommit then
			log.panic(string.format('tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?', tocommit, self:lastIndex()))
		end
		self.committed = tocommit
	end
end

function this:appliedTo(i)
	if i == 0 then return end
	if self.committed < i or i < self.applied then
		log.panic(string.format('applied(%d) is out of range [prevApplied(%d), committed(%d)]', i, self.applied, self.committed))
	end
	self.applied = i
end

function this:stableTo(i, t)
	self.unstable:stableTo(i, t)
end

function this:stableSnapTo(i)
	self.unstable.stableSnapTo(i)
end

function this:lastTerm()
	local t, err = self:term(self:lastIndex())
	if err then
		log.panic('unexpected error when getting the last term (' .. err .. ')')
	end
	return t
end

function this:term(i)
	local dummyIndex = self:firstIndex - 1
	if i < dummyIndex or i > self.lastIndex() then
		return 0
	end
	local t, ok = self.unstable:maybeTerm(i)
	if ok then
		return t
	end
	local t, err = self.storage:term(i)
	if not err then
		return t
	end
	if err == g.errCode.COMPACTED or err == g.errCode.UNAVALIABLE then
		return 0, err
	end
	log.panic(err)
end

function this:entries(i, maxSize)
	if i > self:lastIndex() then
		return
	end
	return self:slice(i, self:lastIndex() + 1, maxSize)
end

function this:allEntries()
	local ents, err = self:entries(self:firstIndex(), g.noLimit)
	if not err then
		return ents
	end
	if err == g.errCode.COMPACTED then
		return self:allEntries()
	end
	log.panic(err)
end

function this:isUpToDate(lasti, term)
	local lt = self:lastTerm()
	return term > lt or (term == lt and lasti >= self:lastIndex())
end

function this:matchTerm(i, term)
	local t, err = self:term(i)
	if err then
		return false
	end
	return t == term
end

function this:maybeCommit(maxIndex, term)
	if maxIndex > self.committed and self:zeroTermOnErrCompacted(self:term(maxIndex)) == term then
		self:commitTo(maxIndex)
		return true
	end
	return false
end

function this:restore(s)
	self:print()
	log.info(string.format('starts to restore snapshot [index:%d, term:%d]', s.metadata.index, s.metadata.term))
	self.committed = s.metadata.index
	self.unstable.restore(s)
end

function this:slice(lo, hi, maxSize)
	local err = self:mustCheckOutOfBounds(lo, hi)
	if err then
		return nil, err
	end
	if lo == hi then
		return
	end
	local ents
	if lo < self.unstable.offset then
		local storedEnts, err = self.storage:entries(lo, math.min(hi, self.unstable.offset), maxSize)
		if err = g.errCode.COMPACTED then
			return nil, err
		elseif err == g.errCode.UNAVALIABLE then
			log.panic(string.format('entries[%d:%d] is unavailable from storage', lo, math.min(hi, self.unstable.offset)))
		elseif err then
			log.panic(err)
		end
		if #storedEnts < math.min(hi, l.unstable.offset) - lo then
			return storedEnts, nil
		end
		ents = storedEnts
	end
	if hi > self.unstable.offset then
		local unstable = self.unstable:slice(math.max(lo, self.unstable.offset), hi)
		if ents then
			local i = #ents
			for j, v in ipairs(unstable) do
				ents[i + j] = v
			end
		else
			ents = unstable
		end
	end
	return g.limitSize(ents, maxSize)
end

function this:mustCheckOutOfBounds(lo, hi)
	if lo > hi then
		log.panic('invalid slice ' .. lo .. ' > ' .. hi)
	end
	local fi = self:firstIndex()
	if lo < fi then
		return g.errCode.COMPACTED
	end
	if hi > self:lastIndex() then
		log.panic(string.format('slice[%d,%d] out of bound [%d,%d]', lo, hi, fi, self:lastIndex()))
	end
end

function this:zeroTermOnErrCompacted(t, err)
	if not err then
		return t
	end
	if err == g.errCode.COMPACTED then
		return 0
	end
	log.panic('unexpected error (' .. err .. ')')
end

return this