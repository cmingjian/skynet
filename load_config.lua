-- skynet启动执行的第一个lua文件,从skynet_main.c中抽取出来
local result = {}
local function getenv(name) return assert(os.getenv(name), [[os.getenv() failed: ]] .. name) end
local sep = package.config:sub(1,1) -- 获目录分隔符
local current_path = [[.]]..sep
local function include(filename)
    local last_path = current_path
    local path, name = filename:match([[(.*]]..sep..[[)(.*)$]])
    if path then
        if path:sub(1,1) == sep then	-- root路径前面就不加./了
            current_path = path
        else
            current_path = current_path .. path
        end
    else
        name = filename
    end
    local f = assert(io.open(current_path .. name))
    local code = assert(f:read [[*a]])  -- 读取整个配置文件
    code = string.gsub(code, [[%$([%w_%d]+)]], getenv)  -- 把配置文件中$开头的符号，替代成环境变量的值！！！
    f:close()
    assert(load(code,[[@]]..filename,[[t]],result))()   -- 执行load所返回的方法
    current_path = last_path
end
setmetatable(result, { __index = { include = include } })
local config_name = ...     -- 命令行中传入的，配置文件的名字
include(config_name)
setmetatable(result, nil)
return result
