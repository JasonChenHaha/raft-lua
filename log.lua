local io = io
local type = type
local ngx = ngx
local ERR = ngx.ERR

--------------------------------------------------------------
local log = {
    name = string.match(debug.getinfo(1).short_src, '.*/(.*).lua'); -- 模块名称
    path = './logs/raft/';
    fd = {};
}
local this = log

this.errText = {
    [1] = 'requested index is unavailable due to compaction';
    [2] = 'requested index is older than the existing snapshot';
    [3] = 'requested entry at index is unavailable';
    [4] = 'snapshot is temporarily unavailable';
    [5] = 'raft: cannot step raft local message';
    [6] = 'raft: cannot step as peer not found';
    [7] = 'raft proposal dropped';
}

local function get_fd(name)
    local filePath = this.path .. name .. '.log'
    if this.fd[filePath] then
        return this.fd[filePath]
    end
    local fd, err = io.open(filePath, 'a+')
    if not fd then
        ngx.log(ERR, err)
        return
    end
    fd:setvbuf('no')
    this.fd[filePath] = fd
    return fd
end

local function release_fd(premature)
    for _, fd in pairs(this.fd) do
        fd:close()
    end
    this.fd = {}
end

-- 设定路径
this.set_path = function(path)
    this.path = path
end

this.info = function(msg)
    local fd = get_fd('info_' .. ngx.localtime():match('(.+) '))
    if not fd then return end
    msg = '[' .. localtime .. '] ' .. msg .. '\n'
    fd:write(msg)
end

this.debug = function(msg)
    local fd = get_fd('debug_' .. ngx.localtime():match('(.+) '))
    if not fd then return end
    msg = '[' .. localtime .. '] ' .. msg .. '\n'
    fd:write(msg)
end

this.warning = function(msg)
    local fd = get_fd('warning_' .. ngx.localtime():match('(.+) '))
    if not fd then return end
    msg = '[' .. localtime .. '] ' .. msg .. '\n'
    fd:write(msg)
end

this.panic = function(err)
    if type(err) == 'number' then
        err = this.errText[err]
    end
    local fd = get_fd('panic_' .. ngx.localtime():match('(.+) '))
    if not fd then return end
    msg = '[' .. localtime .. '] ' .. msg .. '\n'
    fd:write(msg)
    ngx.exit(0)
end

ngx.timer.every(7 * 86400, release_fd)
return this