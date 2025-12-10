### TODO LIST

1. ~~Add `ECONNRESET` error handling~~
2. ~~Add `RequestID` to packets~~
3. ~~Lua packet dissector~~

Implement Open Loop:

4. ~~Open loop client, spin polling connections~~
5. Server that can handle open loop clients, namely:
    * ~~Has bounded packet queues~~
    * ~~Drop responses~~
    * Graceful error handling
        * Closed connections (incl., closed channels)
        * Add backtraces?
6. Client edits
    * Multi-packet read support
    * Epoll support for Linux