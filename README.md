# PMQ
## A Message Queue Broker implemented totally in python

PMQ is a pub-sub message broker. The publish-subscribe pattern, often called the pub-sub pattern involves publishers that produce  messages in different categories and subscribers who consume published messages from various categories they are subscribed to. A message will only be deleted if it's consumed by all subscribers to the category.

## pub/sub sequence diagram
```mermaid
sequenceDiagram
    broker->>broker:start to accept
    consumer->>broker:subscribe a topic
    alt topic dose not exists
        broker->>+queue:create the topic queue synchronized
        queue-->>-broker:return topic queue
    end
    broker-->>consumer:subscribe ok
    loop wait and receive message
        broker->>queue:get message with block until an item is available
        queue-->>broker:return message
        broker->>consumer:send message
        consumer->>broker:receive message
    end
    producer->>broker:submit a message with topic
    alt topic dose not exists
        broker->>+queue:create the topic queue synchronized
        queue-->>-broker:return topic queue
    end
    broker->>queue:put message into topic queue
    broker-->>producer:message delivered succeed
    producer->>producer:acknowledge
    par
        producer->>producer:close connection
        broker->>broker:close connection with producer
    end
```