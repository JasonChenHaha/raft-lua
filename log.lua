local io = io
local ngx = ngx
local ERR = ngx.ERR

--------------------------------------------------------------
local log = {
    name = string.match(debug.getinfo(1).short_src, '.*/(.*).lua'); -- 模块名称
    path = './logs/raft/';
    fd = {};
}
local this = log

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

-- 设定路径
function this.set_path(path)
    this.path = path
end

-- 输出日志到"带当前日期后缀的名字的"文件，文件不存在则创建
function this.print(msg)
    local fd = get_fd('raft_' .. ngx.localtime():match('(.+) '))
    if not fd then return end
    msg = '[' .. localtime .. '] ' .. msg .. '\n'
    fd:write(msg)
end

return this