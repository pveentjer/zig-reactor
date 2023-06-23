# zig-reactor
An io_uring based reactor in Zig. This is a toy project. The goal of this project is 
to learn Zig and to provide a performance baseline for a similar system I'm developing
in Java at Hazelcast. The project uses Zig 0.10.1.

The project contains an echo benchmark where messages are being 
echoed between a client and server. To run the this echo benchmark:

    zig build run

Or with full optimizations:

    zig build -Drelease-fast=true run

There are many things that need to be done:
- Multiple sockets connections
- Concurrent payloads from a single connection
- Configurable payload size
- Improved error handling
- Code cleanup
- Reactor tasks with a deadline
- CPU pinning
- No need for random port
- etc.