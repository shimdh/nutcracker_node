nutcracker - a node.js [nutcracker(twemproxy)](https://github.com/twitter/twemproxy) client
====

nutcracker has a limitation on available commands (e.g., "info", "keys" and [more](https://github.com/twitter/twemproxy/blob/master/notes/redis.md)).

So you cannot use [node_redis](https://github.com/mranney/node_redis) as it is. This module wraps these unavailable commands, and if these commands were called, return warning message and errors.

Now it was added redis stream for using twemproxy.

Installation
---

```bash
npm install node-nutcracker
```

Usage
---

```javascript
var nutcracker = require("node-nutcracker");
var client = nutcracker.createClient(22121, "127.0.0.1");

client.set("string key", "string val", nutcracker.print);
client.keys("*", nutcracker.print);

var redisStream = nutcracker.Redis;

```