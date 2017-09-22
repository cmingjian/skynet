local c = require "skynet.core"
local tostring = tostring
local tonumber = tonumber
local coroutine = coroutine
local assert = assert
local pairs = pairs
local pcall = pcall

local profile = require "profile"

local coroutine_resume = profile.resume	-- 功能和coroutine.resume一模一样, profile模块封装了记录时间的操作
local coroutine_yield = profile.yield	-- 功能和coroutine.yield一模一样，同上

local proto = {}
local skynet = {
	-- read skynet.h
	PTYPE_TEXT = 0,
	PTYPE_RESPONSE = 1,
	PTYPE_MULTICAST = 2,
	PTYPE_CLIENT = 3,
	PTYPE_SYSTEM = 4,
	PTYPE_HARBOR = 5,
	PTYPE_SOCKET = 6,
	PTYPE_ERROR = 7,
	PTYPE_QUEUE = 8,	-- used in deprecated mqueue, use skynet.queue instead
	PTYPE_DEBUG = 9,
	PTYPE_LUA = 10,
	PTYPE_SNAX = 11,
}

-- code cache
skynet.cache = require "skynet.codecache"

function skynet.register_protocol(class)
	local name = class.name
	local id = class.id
	assert(proto[name] == nil)
	assert(type(name) == "string" and type(id) == "number" and id >=0 and id <=255)
	proto[name] = class
	proto[id] = class
end

local session_id_coroutine = {}
local session_coroutine_id = {}
local session_coroutine_address = {}
local session_response = {}
local unresponse = {}

local wakeup_session = {}
local sleep_session = {}

local watching_service = {}
local watching_session = {}
local dead_service = {}
local error_queue = {}
local fork_queue = {}

-- suspend is function
-- 在下面有定义
local suspend

local function string_to_handle(str)
	return tonumber("0x" .. string.sub(str , 2))
end

----- monitor exit

local function dispatch_error_queue()
	local session = table.remove(error_queue,1)
	if session then
		local co = session_id_coroutine[session]
		session_id_coroutine[session] = nil
		return suspend(co, coroutine_resume(co, false))
	end
end

local function _error_dispatch(error_session, error_source)
	if error_session == 0 then
		-- service is down
		--  Don't remove from watching_service , because user may call dead service
		if watching_service[error_source] then
			dead_service[error_source] = true
		end
		for session, srv in pairs(watching_session) do
			if srv == error_source then
				table.insert(error_queue, session)
			end
		end
	else
		-- capture an error for error_session
		if watching_session[error_session] then
			table.insert(error_queue, error_session)
		end
	end
end

-- coroutine reuse

local coroutine_pool = setmetatable({}, { __mode = "kv" })

-- 不直接使用coroutine.create，是为了避免频繁创建协程
-- 之所以要复用coroutine（而不是每处理一个消息创建一个协程），是因为如果每处理一个消息创建一个协程，内存会等到luaGC的时候才被回收
-- https://blog.codingnow.com/2013/07/coroutine_reuse.html
-- 同时要先理解协程的工作方式,对于coroutine.resume (co [, val1, ···])
-- 当你第一次resume一个协程，它会从主体函数处开始运行。 val1, ... 这些值会以参数形式传入主体函数。
-- 如果该协程被让出，resume 会重新启动它； val1, ... 这些参数会作为让出点的返回值。

-- 参考：http://cloudwu.github.io/lua53doc/manual.html#pdf-coroutine.resume
-- 两种情况：
-- 情况1.coroutine_pool为空，创建协程co，
-- 在raw_dispatch_message中调用coroutine_resume时，co中的匿名函数被调用，处理消息的方法f被调用，
-- 其后f=nil，并将协程放到coroutine_pool，通过coroutine_yield返回 true, "EXIT"
-- 不管是新建协程还是复用协程的流程，调用 coroutine_yield "EXIT"后都会返回到 raw_dispatch_message 的第35行，执行 suspend 后此次流程才算真正的完成
-- 情况2.coroutine_pool不为空，先从数组中取出 coroutine，（注意这种情况下coroutine_resume会调用两次）
-- 第一次调用coroutine_resume在co_create中，参数只有f（即消息的处理函数）,（参考lua文档，coroutine.resume作用是，如果该协程被让出，
-- resume 会重新启动它； resume除了第一个以外的参数，会作为让出点的返回值。传递给co中的上值func
-- 第二次调用coroutine_resume在raw_dispatch_message中，参数是消息处理函数的参数，调用消息处理函数，
local function co_create(f)
	local co = table.remove(coroutine_pool)		-- 先从数组中取出 coroutine ，从数组中删除是禁止此 coroutine 被其他消息使用
	if co == nil then
		co = coroutine.create(function(...)
			local func = f
			func(...)	-- 执行我们传入的函数
			while true do
				-- 执行完后回收 coroutine
				func = nil
				coroutine_pool[#coroutine_pool+1] = co
				func = coroutine_yield "EXIT"
				func(coroutine_yield())
			end
		end)
	else
		coroutine_resume(co, f)
	end
	return co
end

local function dispatch_wakeup()
	local co = next(wakeup_session)
	if co then
		wakeup_session[co] = nil
		local session = sleep_session[co]
		if session then
			session_id_coroutine[session] = "BREAK"
			return suspend(co, coroutine_resume(co, false, "BREAK"))
		end
	end
end

local function release_watching(address)
	local ref = watching_service[address]
	if ref then
		ref = ref - 1
		if ref > 0 then
			watching_service[address] = ref
		else
			watching_service[address] = nil
		end
	end
end

-- suspend is local function
function suspend(co, result, command, param, size)
	if not result then
		local session = session_coroutine_id[co]
		if session then -- coroutine may fork by others (session is nil)
			local addr = session_coroutine_address[co]
			if session ~= 0 then
				-- only call response error
				c.send(addr, skynet.PTYPE_ERROR, session, "")
			end
			session_coroutine_id[co] = nil
			session_coroutine_address[co] = nil
		end
		error(debug.traceback(co,tostring(command)))
	end
	if command == "CALL" then
		session_id_coroutine[param] = co	-- 发起CALL的时候,会把协程存起来,使用的代码见raw_dispatch_message
	elseif command == "SLEEP" then
		session_id_coroutine[param] = co
		sleep_session[co] = param
	elseif command == "RETURN" then
		local co_session = session_coroutine_id[co]
		if co_session == 0 then	-- 如果send的消息中调用了,repack,就会执行到这里, 是消息回调写错了
			if size ~= nil then
				c.trash(param, size)	-- 要把retpack的结果回收掉
			end
			return suspend(co, coroutine_resume(co, false))	-- 并向发送错误消息给自己, send don't need ret
		end
		local co_address = session_coroutine_address[co]
		if param == nil or session_response[co] then	-- 返回的结果为空是不允许的
			error(debug.traceback(co))
		end
		session_response[co] = true
		local ret
		if not dead_service[co_address] then
			-- 将返回结果param和消息大小size,作为PTYPE_RESPONSE这类消息的参数发送到源地址
			-- 源地址是被call方在处理消息的回调raw_dispatch_message中记录下来的
			-- call发起方在PTYPE_RESPONSE的回调中raw_dispatch_message
			ret = c.send(co_address, skynet.PTYPE_RESPONSE, co_session, param, size) ~= nil
			if not ret then
				-- If the package is too large, returns nil. so we should report error back
				c.send(co_address, skynet.PTYPE_ERROR, co_session, "")
			end
		elseif size ~= nil then
			c.trash(param, size)
			ret = false
		end
		return suspend(co, coroutine_resume(co, ret))
	elseif command == "RESPONSE" then
		local co_session = session_coroutine_id[co]
		local co_address = session_coroutine_address[co]
		if session_response[co] then
			error(debug.traceback(co))
		end
		local f = param
		local function response(ok, ...)
			if ok == "TEST" then
				if dead_service[co_address] then
					release_watching(co_address)
					unresponse[response] = nil
					f = false
					return false
				else
					return true
				end
			end
			if not f then
				if f == false then
					f = nil
					return false
				end
				error "Can't response more than once"
			end

			local ret
			if not dead_service[co_address] then
				if ok then
					ret = c.send(co_address, skynet.PTYPE_RESPONSE, co_session, f(...)) ~= nil
					if not ret then
						-- If the package is too large, returns false. so we should report error back
						c.send(co_address, skynet.PTYPE_ERROR, co_session, "")
					end
				else
					ret = c.send(co_address, skynet.PTYPE_ERROR, co_session, "") ~= nil
				end
			else
				ret = false
			end
			release_watching(co_address)
			unresponse[response] = nil
			f = nil
			return ret
		end
		watching_service[co_address] = watching_service[co_address] + 1
		session_response[co] = true
		unresponse[response] = true
		return suspend(co, coroutine_resume(co, response))
	elseif command == "EXIT" then
		-- coroutine exit
		local address = session_coroutine_address[co]
		release_watching(address)
		session_coroutine_id[co] = nil
		session_coroutine_address[co] = nil
		session_response[co] = nil
	elseif command == "QUIT" then
		-- service exit
		return
	elseif command == "USER" then
		-- See skynet.coutine for detail
		error("Call skynet.coroutine.yield out of skynet.coroutine.resume\n" .. debug.traceback(co))
	elseif command == nil then
		-- debug trace
		return
	else
		error("Unknown command : " .. command .. "\n" .. debug.traceback(co))
	end
	dispatch_wakeup()
	dispatch_error_queue()
end

function skynet.timeout(ti, func)
	local session = c.intcommand("TIMEOUT",ti)
	assert(session)
	local co = co_create(func)
	assert(session_id_coroutine[session] == nil)
	session_id_coroutine[session] = co
end

function skynet.sleep(ti)
	local session = c.intcommand("TIMEOUT",ti)
	assert(session)
	local succ, ret = coroutine_yield("SLEEP", session)
	sleep_session[coroutine.running()] = nil
	if succ then
		return
	end
	if ret == "BREAK" then
		return "BREAK"
	else
		error(ret)
	end
end

function skynet.yield()
	return skynet.sleep(0)
end

function skynet.wait(co)
	local session = c.genid()
	local ret, msg = coroutine_yield("SLEEP", session)
	co = co or coroutine.running()
	sleep_session[co] = nil
	session_id_coroutine[session] = nil
end

local self_handle
function skynet.self()
	if self_handle then
		return self_handle
	end
	self_handle = string_to_handle(c.command("REG"))
	return self_handle
end

function skynet.localname(name)
	local addr = c.command("QUERY", name)
	if addr then
		return string_to_handle(addr)
	end
end

skynet.now = c.now

local starttime

function skynet.starttime()
	if not starttime then
		starttime = c.intcommand("STARTTIME")
	end
	return starttime
end

function skynet.time()
	return skynet.now()/100 + (starttime or skynet.starttime())
end

function skynet.exit()
	fork_queue = {}	-- no fork coroutine can be execute after skynet.exit
	skynet.send(".launcher","lua","REMOVE",skynet.self(), false)
	-- report the sources that call me
	for co, session in pairs(session_coroutine_id) do
		local address = session_coroutine_address[co]
		if session~=0 and address then
			c.redirect(address, 0, skynet.PTYPE_ERROR, session, "")
		end
	end
	for resp in pairs(unresponse) do
		resp(false)
	end
	-- report the sources I call but haven't return
	local tmp = {}
	for session, address in pairs(watching_session) do
		tmp[address] = true
	end
	for address in pairs(tmp) do
		c.redirect(address, 0, skynet.PTYPE_ERROR, 0, "")
	end
	c.command("EXIT")
	-- quit service
	coroutine_yield "QUIT"
end

function skynet.getenv(key)
	return (c.command("GETENV",key))
end

function skynet.setenv(key, value)
	c.command("SETENV",key .. " " ..value)
end

function skynet.send(addr, typename, ...)
	local p = proto[typename]
	return c.send(addr, p.id, 0 , p.pack(...))	-- 返回新消息的session
	-- 第三个参数为session，这里为0是因为skynet.send不需要返回值，所以发送完后这里流程就完全结束了。A的此次发送消息任务已经完全完成了。
end

skynet.genid = assert(c.genid)

skynet.redirect = function(dest,source,typename,...)
	return c.redirect(dest, source, proto[typename].id, ...)
end

skynet.pack = assert(c.pack)
skynet.packstring = assert(c.packstring)
skynet.unpack = assert(c.unpack)
skynet.tostring = assert(c.tostring)
skynet.trash = assert(c.trash)

-- 理解call消息的处理流程,关键在于被call方的消息处理函数中必须调用skynet.retpack
local function yield_call(service, session)
	watching_session[session] = service		-- 1.表示正在watching，另一个服务service
	local succ, msg, sz = coroutine_yield("CALL", session)	-- yield的返回值,是下次resume传进来的参数
	watching_session[session] = nil
	if not succ then
		error "call failed"
	end
	return msg,sz
end

function skynet.call(addr, typename, ...)
	local p = proto[typename]
	local session = c.send(addr, p.id , nil , p.pack(...))
	-- 由于skynet.call是需要返回值的，所以c.send的第三个参数nil表示由框架自动分配一个session
	-- 以便返回时根据相应的session找到对应的协程进行处理
	if session == nil then
		error("call to invalid address " .. skynet.address(addr))
	end
	return p.unpack(yield_call(addr, session))
end

function skynet.rawcall(addr, typename, msg, sz)
	local p = proto[typename]
	local session = assert(c.send(addr, p.id , nil , msg, sz), "call to invalid address")
	return yield_call(addr, session)
end

function skynet.ret(msg, sz)
	msg = msg or ""
	return coroutine_yield("RETURN", msg, sz)
	-- 会让出到raw_dispatch_message函数中，参数作为resume的返回值, 消息处理函数调用skynet.repack以后,
	-- suspend(co, coroutine_resume(co, session,source, p.unpack(msg,sz)))处继续执行:suspend(co, true, "RETURN", msg, sz)
end

function skynet.response(pack)
	pack = pack or skynet.pack
	return coroutine_yield("RESPONSE", pack)
end

function skynet.retpack(...)
	return skynet.ret(skynet.pack(...))
end

function skynet.wakeup(co)
	if sleep_session[co] and wakeup_session[co] == nil then
		wakeup_session[co] = true
		return true
	end
end

function skynet.dispatch(typename, func)
	local p = proto[typename]
	if func then
		local ret = p.dispatch
		p.dispatch = func		-- 设置.dispatch即可，因为在require“skynet”的时候，会调用skynet.register_protocol，注册lua消息
								-- (见本文件后续，----- register protocol以后的部分代码)
		return ret
	else
		return p and p.dispatch
	end
end

local function unknown_request(session, address, msg, sz, prototype)
	skynet.error(string.format("Unknown request (%s): %s", prototype, c.tostring(msg,sz)))
	error(string.format("Unknown session : %d from %x", session, address))
end

function skynet.dispatch_unknown_request(unknown)
	local prev = unknown_request
	unknown_request = unknown
	return prev
end

local function unknown_response(session, address, msg, sz)
	skynet.error(string.format("Response message : %s" , c.tostring(msg,sz)))
	error(string.format("Unknown session : %d from %x", session, address))
end

function skynet.dispatch_unknown_response(unknown)
	local prev = unknown_response
	unknown_response = unknown
	return prev
end

function skynet.fork(func,...)
	local args = table.pack(...)
	local co = co_create(function()
		func(table.unpack(args,1,args.n))
	end)
	table.insert(fork_queue, co)
	return co
end

local function raw_dispatch_message(prototype, msg, sz, session, source)
	-- skynet.PTYPE_RESPONSE = 1, read skynet.h
	if prototype == 1 then
		local co = session_id_coroutine[session]
		if co == "BREAK" then
			session_id_coroutine[session] = nil
		elseif co == nil then
			unknown_response(session, source, msg, sz)
		else
			session_id_coroutine[session] = nil
			-- 将 true, msg, sz作为参数,传递给上次yeild出的返回值,继续执行
			-- 即yeild_call中coroutine_yield("CALL", session)的返回值
			suspend(co, coroutine_resume(co, true, msg, sz))
		end
	else
		local p = proto[prototype]
		if p == nil then
			if session ~= 0 then
				c.send(source, skynet.PTYPE_ERROR, session, "")
			else
				unknown_request(session, source, msg, sz, prototype)
			end
			return
		end
		local f = p.dispatch
		if f then
			local ref = watching_service[source]
			if ref then
				watching_service[source] = ref + 1
			else
				watching_service[source] = 1
			end
			local co = co_create(f)
			session_coroutine_id[co] = session
			session_coroutine_address[co] = source
			-- coroutine_resume 返回的参数(true, "EXIT")给 suspend 作为参数
			suspend(co, coroutine_resume(co, session,source, p.unpack(msg,sz)))
		elseif session ~= 0 then
			c.send(source, skynet.PTYPE_ERROR, session, "")
		else
			unknown_request(session, source, msg, sz, proto[prototype].name)
		end
	end
end

-- 服务接受到消息以后，这个回调会被执行，原因：见skynet.start
function skynet.dispatch_message(...)
	local succ, err = pcall(raw_dispatch_message,...)
	while true do
		-- 如果有skynet.fork创建的协程，在处理完消息以后，suspend这些协程
		local key,co = next(fork_queue)
		if co == nil then
			break
		end
		fork_queue[key] = nil
		local fork_succ, fork_err = pcall(suspend,co,coroutine_resume(co))
		if not fork_succ then
			if succ then
				succ = false
				err = tostring(fork_err)
			else
				err = tostring(err) .. "\n" .. tostring(fork_err)
			end
		end
	end
	assert(succ, tostring(err))
end

function skynet.newservice(name, ...)
	return skynet.call(".launcher", "lua" , "LAUNCH", "snlua", name, ...)
end

function skynet.uniqueservice(global, ...)
	if global == true then
		return assert(skynet.call(".service", "lua", "GLAUNCH", ...))
	else
		return assert(skynet.call(".service", "lua", "LAUNCH", global, ...))
	end
end

function skynet.queryservice(global, ...)
	if global == true then
		return assert(skynet.call(".service", "lua", "GQUERY", ...))
	else
		return assert(skynet.call(".service", "lua", "QUERY", global, ...))
	end
end

function skynet.address(addr)
	if type(addr) == "number" then
		-- 获取":01000003"形式的地址(16位整数,至少8位,前面的0也显示)
		return string.format(":%08x",addr)
	else
		return tostring(addr)
	end
end

function skynet.harbor(addr)
	return c.harbor(addr)
end

skynet.error = c.error

----- register protocol
do
	local REG = skynet.register_protocol

	REG {
		name = "lua",
		id = skynet.PTYPE_LUA,
		pack = skynet.pack,
		unpack = skynet.unpack,
	}

	REG {
		name = "response",
		id = skynet.PTYPE_RESPONSE,
	}

	REG {
		name = "error",
		id = skynet.PTYPE_ERROR,
		unpack = function(...) return ... end,
		dispatch = _error_dispatch,
	}
end

local init_func = {}

function skynet.init(f, name)
	assert(type(f) == "function")
	if init_func == nil then
		f()
	else
		table.insert(init_func, f)
		if name then
			assert(type(name) == "string")
			assert(init_func[name] == nil)
			init_func[name] = f
		end
	end
end

local function init_all()
	local funcs = init_func
	init_func = nil
	if funcs then
		for _,f in ipairs(funcs) do
			f()
		end
	end
end

local function ret(f, ...)
	f()
	return ...
end

local function init_template(start, ...)
	init_all()
	init_func = {}
	return ret(init_all, start(...))
end

function skynet.pcall(start, ...)
	return xpcall(init_template, debug.traceback, start, ...)
end

function skynet.init_service(start)
	local ok, err = skynet.pcall(start)
	if not ok then
		skynet.error("init service failed: " .. tostring(err))
		skynet.send(".launcher","lua", "ERROR")
		skynet.exit()
	else
		skynet.send(".launcher","lua", "LAUNCHOK")
	end
end

-- 通常使用的方式,可以再start_func中加入对dispatch的调用，比如：
--[[
	skynet.dispatch("lua", function(_,_, command, ...)
		local f = CMD[command]
		skynet.ret(skynet.pack(f(...)))
	end)
 ]]

function skynet.start(start_func)
	c.callback(skynet.dispatch_message)		-- 调用完以后,后续消息都使用这个回调方法来处理
	skynet.timeout(0, function()
		skynet.init_service(start_func)
	end)
end

function skynet.endless()
	return (c.intcommand("STAT", "endless") == 1)
end

function skynet.mqlen()
	return c.intcommand("STAT", "mqlen")
end

function skynet.stat(what)
	return c.intcommand("STAT", what)
end

function skynet.task(ret)
	local t = 0
	for session,co in pairs(session_id_coroutine) do
		if ret then
			ret[session] = debug.traceback(co)
		end
		t = t + 1
	end
	return t
end

function skynet.term(service)
	return _error_dispatch(0, service)
end

function skynet.memlimit(bytes)
	debug.getregistry().memlimit = bytes
	skynet.memlimit = nil	-- set only once
end

-- Inject internal debug framework
local debug = require "skynet.debug"
debug.init(skynet, {
	dispatch = skynet.dispatch_message,
	suspend = suspend,
})

return skynet
