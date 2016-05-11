#   node-nutcracker
#   (c) 2016-2016 Daniel Shim, JH, solilove, DocumentCloud and Investigative Reporters & Editors
#   node-nutcracker may be freely distributed under the MIT license.
redis = require "redis"
net = require "net"
util = require "util"
es = require "event-stream"
default_port = 22121
default_host = "127.0.0.1"
commands = ["keys", "migrate", "move", "object", "randomkey", "rename", "renamenx", "sort", "bitop", "mget", "mset",
  "blpop", "brpop", "brpoplpush", "psubscribe", "publish", "punsubscribe", "subscribe", "unsubscribe", "discard",
  "exec", "multi", "unwatch", "watch", "auth", "echo", "ping", "quit", "select", "script exists", "script flush",
  "script kill", "script load", "bgrewriteaof", "bgsave", "client kill", "client list", "config get", "config set",
  "config resetstat", "dbsize", "debug object", "debug segfault", "flushall", "flushdb", "info", "lastsave", "monitor",
  "save", "shutdown", "slaveof", "slowlog", "sync", "time"]

formatString1 = '*%d\r\n'
formatString2 = '$%d\r\n%s\r\n'
replace1 = /^\$[0-9]+/
replace2 = /^\*[0-9]+|^\:|^\+|^\$|^\r\n$/

on_info_cmd = (err, res) ->
  if err
    this.emit("error", new Error("Ready check failed: " + err.message))
  else
    this.on_ready()

exports.RedisClient = redis.RedisClient
exports.createClient = (port_arg, host_arg, options) ->
  port = port_arg || default_port
  host = host_arg || default_host
  net_client = net.createConnection(port, host)
  redis_client = new redis.RedisClient(net_client, options)
  redis_client.port = port
  redis_client.host = host

  redis_client.on_info_cmd = on_info_cmd
  commands.forEach (cmd) ->
    if cmd is "info"
      fn = (array, callback) ->
        console.warn("nutcracker: cannot use " + cmd + " command")
        this.on_info_cmd()
        false
    else
      fn = (array, callback) ->
        if callback && (typeof callback == "function")
          err = new Error('nutcracker: cannot use ' + cmd + ' command')
          callback(err, null)
        false
    redis_client[cmd] = redis_client[cmd.toUpperCase()] = fn
  redis_client

exports.print = redis.print
exports.debug_mode = redis.debug_mode
exports.Multi = redis.Multi


class Redis
  constructor: (@port = 6379, @host = 'localhost', @pw = null) ->
    @

  @es: es

  createConnection: ->
    net.createConnection @port, @host

  stream: (cmd, key, curry) ->
    curry = Array.prototype.slice.call(arguments)
    _redis = @createConnection()

    replyParser = (data, fn) =>
      str = (data+'').replace(replace1, '').replace(replace2, '')
      if Redis.debug_mode then console.log('replyParser', data+'')
      if not str.length
        fn()
      else
        return fn() if @pw?
        fn null, str

    stream = es.pipe(
      es.pipe(
        es.map((data, fn) ->
          elems = [].concat(stream.curry)
          str = data+''
          if not str.length then fn() else elems.push str
          Redis.parse elems, fn
        ),
        _redis
      ),
      es.pipe(
        es.split('\r\n'),
        es.map(replyParser)
      )
    )
    stream.curry = curry
    stream.redis = _redis
    if @pw? then stream.redis.write(Redis.parse(['auth', @pw]))
    stream

  @parse: (elems, fn) ->
    retval = util.format formatString1, elems.length
    while elems.length
      retval += util.format(formatString2, Buffer.byteLength(elems[0]+''), elems.shift()+'')
    if Redis.debug_mode then console.log('commandParser', retval)
    fn and fn(null, retval)
    retval


module.exports.Redis = Redis