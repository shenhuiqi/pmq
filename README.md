# pmq
## a message queue broker implemented totally in python

```mermaid
sequenceDiagram
    consumer->>broker:subscribe a topic
    procedure->>broker:send a message with topic
    broker->>queue:save message
    broker->>queue:pick a message
    queue-->>broker:return the message
    broker->>consumer:send the message
```