# nats.zig - [Zig](https://ziglang.org/) client for the [NATS](https://nats.io) messaging system

## About

I needed concrete problem to learn some Zig. It was also opportunity to learn some details of the NATS protocol.

<!--
NATS protocol [Parser](src/Parser.zig) is pretty much complete. Does not handle message headers but other operations sent by server: INFO, MSG, OK, ERR, PING, PONG are implemented, modeled on Go implementation, covered by tests. 

The rest of the project, [Conn](src/conn.zig) is just bare minimum to get the connection to the NATS server and be able to publish/subscribe. It connects to the local NATS server, without any authentication, handles info/connect handshake, responds to the pongs and provides interface to publish and subscribe.   

I started with the evented version but the switched to the threaded. Zig is currently more complete there, event loop is still in the early sage.   
Conn creates separate thread for reading from the TCP connection and parsing incoming bytes into operations. The rest is handled in the main thread. Those threads are connected by the [RingBuffer](src/RingBuffer.zig) in which parser writes operations and Conn reads from it. 
-->

Project depends on libressl for handling tls connections. Can be built using libressl from project or system. 

To install libressl on macos:

``` sh
brew install libressl
```
and on linux: 

``` sh
wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-3.6.0.tar.gz
tar -xf libressl-3.6.0.tar.gz
ls -al
cd libressl-3.6.0/
./configure
sudo make install
```

After that project can be build using system libressl:

``` sh
zig build -Duse-system-libressl 
```

## Try

Build project:

``` sh
git clone --recurse-submodules git@github.com:ianic/nats.zig.git
cd nats.zig
zig build
```
Tested only using latest master 0.10-dev [release](https://ziglang.org/download/).

You should have [installed](https://docs.nats.io/running-a-nats-service/introduction/installation) nats-server. Start it locally:

``` sh
nats-server
```
Start subscriber in one terminal:

``` sh
./zig-out/bin/sync_sub
```
and publisher to send few messages to the `foo` subject on which subscriber is listening:

``` sh
./zig-out/bin/sync_pub
```

<!--
Or run test binary which both subscribes and publishes to the `test` subject:
``` sh
./zig-out/bin/test
```


<!--
## Usage

``` zig
const nats = @import("nats");

// establish nats connection
var nc = try nats.connect(alloc);
defer nc.deinit();

// publish buf to the foo subject
try nc.publish("foo", buf); 

// subscribe
var sid = try nc.subscribe("foo");

// consume messages 
while (nc.read()) |msg| {
    // ...handle message
    msg.deinit(alloc);
}

// unsubscribe
try nc.unsubscribe(sid);
    
```
-->

## References

* [zig-libressl](https://github.com/haze/zig-libressl)
* [nats protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol) documentation (that page is referencing also Derek's [talk](https://www.youtube.com/watch?v=ylRKac5kSOk&t=646s) about zero allocation parser)  
* [zig-nats](https://github.com/rutgerbrf/zig-nats) by Rutger Broekhoff   
* Twitter [conversation](https://mobile.twitter.com/derekcollison/status/1410600465302052870)  




