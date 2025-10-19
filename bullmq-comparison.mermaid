graph TB
    subgraph BullMQ_Features["BullMQ (Redis-based)"]
        BQ[BullMQ<br/>Core Technology]
        BQ --> BF1[✓ Lightweight & Fast]
        BQ --> BF2[✓ Priority Queues]
        BQ --> BF3[✓ Delayed Jobs]
        BQ --> BF4[✓ Rate Limiting]
        BQ --> BF5[✓ Job Dependencies]
        BQ --> BF6[✓ Minimal Infrastructure]
        BQ --> BF7[✓ Great for Node.js]
        
        BQ -.-> BL1[✗ Single Point of Failure<br/>without Redis Cluster]
        BQ -.-> BL2[✗ Memory-based Storage]
        BQ -.-> BL3[✗ Limited Message Size]
    end
    
    subgraph RabbitMQ_Features["RabbitMQ (AMQP)"]
        RMQ[RabbitMQ<br/>Core Technology]
        RMQ --> RF1[✓ Battle-tested]
        RMQ --> RF2[✓ Complex Routing]
        RMQ --> RF3[✓ Multiple Protocols]
        RMQ --> RF4[✓ Disk Persistence]
        RMQ --> RF5[✓ Federation & Clustering]
        
        RMQ -.-> RL1[✗ Higher Resource Usage]
        RMQ -.-> RL2[✗ Complex Configuration]
        RMQ -.-> RL3[✗ Erlang Dependency]
        RMQ -.-> RL4[✗ Steeper Learning Curve]
    end
    
    subgraph Kafka_Features["Apache Kafka"]
        KFK[Kafka<br/>Core Technology]
        KFK --> KF1[✓ Massive Throughput]
        KFK --> KF2[✓ Event Streaming]
        KFK --> KF3[✓ Log Retention]
        KFK --> KF4[✓ Replay Capability]
        KFK --> KF5[✓ Distributed by Design]
        
        KFK -.-> KL1[✗ Heavy Infrastructure]
        KFK -.-> KL2[✗ Operational Complexity]
        KFK -.-> KL3[✗ Overkill for Simple Queues]
        KFK -.-> KL4[✗ No Built-in Job Retry]
    end
    
    subgraph SQS_Features["AWS SQS"]
        SQS[SQS<br/>Core Technology]
        SQS --> SF1[✓ Fully Managed]
        SQS --> SF2[✓ Auto-scaling]
        SQS --> SF3[✓ High Availability]
        SQS --> SF4[✓ Pay-per-use]
        
        SQS -.-> SL1[✗ AWS Lock-in]
        SQS -.-> SL2[✗ Limited Features]
        SQS -.-> SL3[✗ No Priority Queues]
        SQS -.-> SL4[✗ Polling Overhead]
    end
    
    subgraph NATS_Features["NATS"]
        NATS[NATS<br/>Core Technology]
        NATS --> NF1[✓ Extremely Fast]
        NATS --> NF2[✓ Lightweight]
        NATS --> NF3[✓ Simple Protocol]
        NATS --> NF4[✓ Multi-language]
        
        NATS -.-> NL1[✗ No Persistence<br/>by default]
        NATS -.-> NL2[✗ Limited Routing]
        NATS -.-> NL3[✗ Fewer Features<br/>for Job Queues]
    end
    
    subgraph Use_Cases["When to Use Each"]
        UC1[BullMQ:<br/>Node.js apps, Job queues,<br/>Task scheduling,<br/>Moderate throughput<br/><br/>Throughput: 10K-100K/sec<br/>Latency: less than 10ms]
        
        UC2[RabbitMQ:<br/>Complex routing,<br/>Enterprise integration,<br/>Multi-protocol needs<br/><br/>Throughput: 10K-50K/sec<br/>Latency: 10-50ms]
        
        UC3[Kafka:<br/>Event streaming,<br/>Big data pipelines,<br/>Log aggregation<br/><br/>Throughput: 100K-1M/sec<br/>Latency: 50-100ms]
        
        UC4[SQS:<br/>AWS-native apps,<br/>Serverless architectures,<br/>No ops overhead<br/><br/>Throughput: Variable<br/>Latency: 100-500ms]
        
        UC5[NATS:<br/>Microservices mesh,<br/>IoT applications,<br/>Real-time messaging<br/><br/>Throughput: 500K-1M/sec<br/>Latency: less than 5ms]
    end
    
    subgraph Decision_Tree["Decision Matrix"]
        DM[Choose Based On<br/>Your Requirements]
        DM --> D1{Already using<br/>Redis?}
        D1 -->|Yes| BQ_CHOICE[Choose BullMQ]
        D1 -->|No| D2{Need event<br/>streaming?}
        
        D2 -->|Yes| D3{Massive<br/>scale?}
        D3 -->|Yes| KFK_CHOICE[Choose Kafka]
        D3 -->|No| BQ_CHOICE2[Choose BullMQ]
        
        D2 -->|No| D4{On AWS?}
        D4 -->|Yes| SQS_CHOICE[Choose SQS]
        D4 -->|No| D5{Complex<br/>routing?}
        
        D5 -->|Yes| RMQ_CHOICE[Choose RabbitMQ]
        D5 -->|No| D6{Ultra-low<br/>latency?}
        
        D6 -->|Yes| NATS_CHOICE[Choose NATS]
        D6 -->|No| BQ_CHOICE3[Choose BullMQ]
    end
    
    %% Styling
    style BQ fill:#4ecdc4,stroke:#1a7a72,stroke-width:4px,color:#000
    style RMQ fill:#ff9f43,stroke:#ee5a24,stroke-width:3px,color:#000
    style KFK fill:#686de0,stroke:#30336b,stroke-width:3px,color:#fff
    style SQS fill:#feca57,stroke:#ff9f43,stroke-width:3px,color:#000
    style NATS fill:#95afc0,stroke:#535c68,stroke-width:3px,color:#000
    
    style UC1 fill:#e8f8f5,stroke:#1abc9c,stroke-width:2px
    style UC2 fill:#fef5e7,stroke:#f39c12,stroke-width:2px
    style UC3 fill:#ebf5fb,stroke:#3498db,stroke-width:2px
    style UC4 fill:#fef9e7,stroke:#f1c40f,stroke-width:2px
    style UC5 fill:#f4f6f7,stroke:#7f8c8d,stroke-width:2px
    
    style BF1 fill:#d4edda,stroke:#28a745
    style BF2 fill:#d4edda,stroke:#28a745
    style BF3 fill:#d4edda,stroke:#28a745
    style BF4 fill:#d4edda,stroke:#28a745
    style BF5 fill:#d4edda,stroke:#28a745
    style BF6 fill:#d4edda,stroke:#28a745
    style BF7 fill:#d4edda,stroke:#28a745
    
    style BL1 fill:#f8d7da,stroke:#dc3545
    style BL2 fill:#f8d7da,stroke:#dc3545
    style BL3 fill:#f8d7da,stroke:#dc3545
    
    style BQ_CHOICE fill:#2ecc71,stroke:#27ae60,stroke-width:3px,color:#fff
    style BQ_CHOICE2 fill:#2ecc71,stroke:#27ae60,stroke-width:3px,color:#fff
    style BQ_CHOICE3 fill:#2ecc71,stroke:#27ae60,stroke-width:3px,color:#fff
    style KFK_CHOICE fill:#9b59b6,stroke:#8e44ad,stroke-width:3px,color:#fff
    style RMQ_CHOICE fill:#e67e22,stroke:#d35400,stroke-width:3px,color:#fff
    style SQS_CHOICE fill:#f39c12,stroke:#e67e22,stroke-width:3px,color:#fff
    style NATS_CHOICE fill:#34495e,stroke:#2c3e50,stroke-width:3px,color:#fff
