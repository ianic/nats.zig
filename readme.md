# nats.zig - [Zig](https://ziglang.org/) client for the [NATS](https://nats.io) messaging system

## About

Minimal synchronous NATS Zig client.

Features:
 * subject subscription
 * message publishing
 * tls connections
 * nkey authentication
 * connecting to the [ngs](https://synadia.com/ngs)
 * non allocating protocol parser
 

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
./zig-out/bin/sub
```
and publisher to send few messages to the `foo` subject on which subscriber is listening:

``` sh
./zig-out/bin/pub
```

## ngs example

To connect and publish/subscribe to the ngs network you need valid credentials file. Start the ngs example and point it to the credentials file and specify a subject name. Example will both subscribe and publish few messages. 

``` sh
./zig-out/bin/ngs credentials_file.creds subject
```

## LibreSSL

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


## References

* [zig-libressl](https://github.com/haze/zig-libressl)
* [nats protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol) documentation (that page is referencing also Derek's [talk](https://www.youtube.com/watch?v=ylRKac5kSOk&t=646s) about zero allocation parser)  
* [zig-nats](https://github.com/rutgerbrf/zig-nats) and [zig-nkeys](https://github.com/rutgerbrf/zig-nkeys) by Rutger Broekhoff   
* Twitter [conversLation](https://mobile.twitter.com/derekcollison/status/1410600465302052870)  




