### TODO LIST

1. ~~Add `ECONNRESET` error handling~~
2. ~~Add `RequestID` to packets~~
3. ~~Lua packet dissector~~

Implement Open Loop:

4. ~~Open loop client, spin polling connections~~
5. Server that can handle open loop clients, namely:
    * Has bounded packet queues
    * Drop responses
6. Client that uses Linux `epoll`
