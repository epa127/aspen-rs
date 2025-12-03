### Things to add

1. Add open-loop tail latency benchmarks
    * Open-loop client and open-loop server
    * Server needs an explicit queue
    * Client needs sender, receiver, and timing threads
    * Server and client needs to handle disconnections

### TODO LIST

1. ~~Add `ECONNRESET` error handling~~
2. Add `RequestID` to packets
3. Add error packet type
4. Lua packet dissector
5. Implement Open Loop
    * Client
        1. Sending thread (with timer)
        2. Receiving thread
        3. Results
    * Server
        1. Packet queues, with drops
6. Multithreaded client support (multiple senders and receivers?)