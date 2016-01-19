var redis = require("redis");
var net = require("net");
var es = require('event-stream');
var util = require('util');
var default_port = 22121;
var default_host = "127.0.0.1";
var commands = ["keys", "migrate", "move", "object", "randomkey", "rename", "renamenx", "sort", "bitop", "mget", "mset", "blpop", "brpop", "brpoplpush", "psubscribe", "publish", "punsubscribe", "subscribe", "unsubscribe", "discard", "exec", "multi", "unwatch", "watch", "auth", "echo", "ping", "quit", "select", "script exists", "script flush", "script kill", "script load", "bgrewriteaof", "bgsave", "client kill", "client list", "config get", "config set", "config resetstat", "dbsize", "debug object", "debug segfault", "flushall", "flushdb", "info", "lastsave", "monitor", "save", "shutdown", "slaveof", "slowlog", "sync", "time"];


var formatString1 = '*%d\r\n';
var formatString2 = '$%d\r\n%s\r\n';
var replace1 = /^\$[0-9]+/;
var replace2 = /^\*[0-9]+|^\:|^\+|^\$|^\r\n$/;

var on_info_cmd = function (err, res) {
    if (err) {
        return this.emit("error", new Error("Ready check failed: " + err.message));
    } else {
        return this.on_ready();
    }
};

exports.RedisClient = redis.RedisClient;

exports.createClient = function (port_arg, host_arg, options) {
    var host, net_client, port, redis_client;
    port = port_arg || default_port;
    host = host_arg || default_host;
    net_client = net.createConnection(port, host);
    redis_client = new redis.RedisClient(net_client, options);
    redis_client.port = port;
    redis_client.host = host;
    redis_client.on_info_cmd = on_info_cmd;

    redis.es = es;

    redis.prototype.stream = function (cmd, key, curry ) {
        var curry = Array.prototype.slice.call(arguments),
            clip = 1,
            _redis = net_client.createConnection(),
            stream = es.pipe(
            es.pipe(
                es.map(function (data, fn) {
                    var elems = [].concat(stream.curry),
                        str = data+'';
                    if (!str.length) return fn();
                    else elems.push(str);
                    // console.log('write', str)
                    return Redis.parse(elems, fn);
                }),
                _redis),
            es.pipe(
                es.split('\r\n'),
                es.map(replyParser)
            )
        );

        stream.curry = curry;
        stream.redis = _redis;
        stream.redis.write(redis.parse([ 'select', this.db ]));

        return stream;

        function replyParser (data, fn) {
            if (redis.debug_mode) console.log('replyParser', data+'');
            var str = (data+'').replace(replace1, '').replace(replace2, '');
            if (!str.length) return fn();
            else if (clip) {
                clip--;
                return fn();
            }
            else return fn(null, str);
        }
    };

    redis.parse = function commandParser (elems, fn) {
        var retval = util.format(formatString1, elems.length);
        while (elems.length) retval += util.format(formatString2, Buffer.byteLength(elems[0]+''), elems.shift()+'');
        if (Redis.debug_mode) console.log('commandParser', retval);
        fn && fn(null, retval);
        return retval
    };

    Redis.parse.hgetall =
        Redis.parse.hmget = function () {
            var hash = {}
                , fields = []
                , vals = []
                , len = 0

            return es.map(function (data, fn) {
                var retval = ''
                if (!(len++ % 2)) fields.push(data)
                else vals.push(String(data))
                if (vals.length === fields.length) {
                    return fn(null, [ fields.pop(), vals.pop() ])
                }
                else {
                    return fn()
                }
            })
        }

    commands.forEach(function (cmd) {
        var fn;
        if (cmd === "info") {
            fn = function (array, callback) {
                console.warn("nutcracker: cannot use " + cmd + " command");
                this.on_info_cmd();
                return false;
            };
        } else {
            fn = function (array, callback) {
                var err;
                if (callback && (typeof callback === "function")) {
                    err = new Error('nutcracker: cannot use ' + cmd + ' command');
                    callback(err, null);
                }
                return false;
            };
        }
        return redis_client[cmd] = redis_client[cmd.toUpperCase()] = fn;
    });
    return redis_client;
};

exports.print = redis.print;

exports.debug_mode = redis.debug_mode;

exports.Multi = redis.Multi;


