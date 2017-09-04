local args = {}
for word in string.gmatch(..., "%S+") do
	table.insert(args, word)
end

SERVICE_NAME = args[1]		-- 默认这里就是lua文件的名字

local main, pattern

local err = {}
for pat in string.gmatch(LUA_SERVICE, "([^;]+);*") do
	local filename = string.gsub(pat, "?", SERVICE_NAME)
	local f, msg = loadfile(filename)
	if not f then
		table.insert(err, msg)
	else	-- 找到有目标文件的路径，并加载该文件
		pattern = pat
		main = f
		break
	end
end

if not main then
	error(table.concat(err, "\n"))
end

LUA_SERVICE = nil
package.path , LUA_PATH = LUA_PATH
package.cpath , LUA_CPATH = LUA_CPATH

-- 如果配置中，配了全路径名的文件（而不是带问号的路径）
-- 比如luaservice = root.."service/bootstrap.lua;"
-- 这种情况下service_path不为nil
local service_path = string.match(pattern, "(.*/)[^/?]+$")

if service_path then	-- 如果路径中不带问号，则将路径设置为带问号的格式
	service_path = string.gsub(service_path, "?", args[1])
	package.path = service_path .. "?.lua;" .. package.path
	SERVICE_PATH = service_path
else
	local p = string.match(pattern, "(.*/).+$")
	SERVICE_PATH = p
end

if LUA_PRELOAD then
	local f = assert(loadfile(LUA_PRELOAD))
	f(table.unpack(args))
	LUA_PRELOAD = nil
end

-- 第一个参数是lua脚本名字，剩余的参数才是该脚本的...参数
main(select(2, table.unpack(args)))
