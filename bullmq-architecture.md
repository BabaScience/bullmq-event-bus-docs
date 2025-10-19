graph TB
    subgraph "Client Layer"
        API[API Gateway/REST Endpoints]
        WEB[Web Application]
        MOBILE[Mobile Application]
    end
    
    subgraph "Microservices Layer"
        OS[Order Service<br/>Publisher]
        IS[Inventory Service<br/>Subscriber]
        PS[Payment Service<br/>Subscriber]
        NS[Notification Service<br/>Subscriber]
        AS[Analytics Service<br/>Subscriber]
        SS[Shipping Service<br/>Subscriber]
    end
    
    subgraph "Event Bus Infrastructure - BullMQ + Redis"
        subgraph "Event Queues"
            Q1[order.created<br/>Queue]
            Q2[inventory.reserved<br/>Queue]
            Q3[payment.processed<br/>Queue]
            Q4[notification.sent<br/>Queue]
            Q5[shipping.scheduled<br/>Queue]
            DLQ[Dead Letter<br/>Queue]
        end
        
        subgraph "Redis Persistence"
            REDIS[(Redis Master<br/>Job Storage<br/>State Management)]
            REPLICA1[(Redis Replica 1)]
            REPLICA2[(Redis Replica 2)]
        end
        
        subgraph "Worker Pool"
            W1[Worker 1<br/>Concurrency: 10]
            W2[Worker 2<br/>Concurrency: 10]
            W3[Worker 3<br/>Concurrency: 10]
            WN[Worker N<br/>Concurrency: 10]
        end
    end
    
    subgraph "Observability Stack"
        PROM[Prometheus<br/>Metrics]
        GRAF[Grafana<br/>Dashboards]
        ELK[ELK Stack<br/>Logs]
        JAEGER[Jaeger<br/>Distributed Tracing]
    end
    
    subgraph "External Systems"
        EMAIL[Email Service<br/>SendGrid/SES]
        SMS[SMS Gateway<br/>Twilio]
        PAYMENT_GW[Payment Gateway<br/>Stripe/PayPal]
        WAREHOUSE[Warehouse System]
    end
    
    %% Client to Services
    API --> OS
    WEB --> API
    MOBILE --> API
    
    %% Service to Queue Publishing
    OS -->|publish event| Q1
    IS -->|publish event| Q2
    PS -->|publish event| Q3
    NS -->|publish event| Q4
    SS -->|publish event| Q5
    
    %% Queue to Redis
    Q1 --> REDIS
    Q2 --> REDIS
    Q3 --> REDIS
    Q4 --> REDIS
    Q5 --> REDIS
    DLQ --> REDIS
    
    %% Redis Replication
    REDIS -.->|replicate| REPLICA1
    REDIS -.->|replicate| REPLICA2
    
    %% Workers consuming from Redis
    REDIS -->|fetch jobs| W1
    REDIS -->|fetch jobs| W2
    REDIS -->|fetch jobs| W3
    REDIS -->|fetch jobs| WN
    
    %% Workers to Services
    W1 -->|process| IS
    W1 -->|process| NS
    W2 -->|process| PS
    W2 -->|process| AS
    W3 -->|process| SS
    WN -->|process| IS
    
    %% Failed jobs to DLQ
    W1 -.->|failures| DLQ
    W2 -.->|failures| DLQ
    W3 -.->|failures| DLQ
    
    %% Services to External Systems
    NS --> EMAIL
    NS --> SMS
    PS --> PAYMENT_GW
    SS --> WAREHOUSE
    
    %% Monitoring
    REDIS -.->|metrics| PROM
    Q1 -.->|metrics| PROM
    Q2 -.->|metrics| PROM
    W1 -.->|logs| ELK
    W2 -.->|logs| ELK
    OS -.->|traces| JAEGER
    IS -.->|traces| JAEGER
    
    PROM --> GRAF
    
    %% Styling
    style REDIS fill:#ff6b6b,stroke:#c92a2a,stroke-width:4px,color:#fff
    style Q1 fill:#4ecdc4,stroke:#1a7a72,stroke-width:3px,color:#000
    style Q2 fill:#4ecdc4,stroke:#1a7a72,stroke-width:3px,color:#000
    style Q3 fill:#4ecdc4,stroke:#1a7a72,stroke-width:3px,color:#000
    style Q4 fill:#4ecdc4,stroke:#1a7a72,stroke-width:3px,color:#000
    style Q5 fill:#4ecdc4,stroke:#1a7a72,stroke-width:3px,color:#000
    style DLQ fill:#ff8787,stroke:#ff0000,stroke-width:3px,color:#000
    style W1 fill:#95e1d3,stroke:#1a7a72,stroke-width:2px
    style W2 fill:#95e1d3,stroke:#1a7a72,stroke-width:2px
    style W3 fill:#95e1d3,stroke:#1a7a72,stroke-width:2px
    style WN fill:#95e1d3,stroke:#1a7a72,stroke-width:2px
    style OS fill:#a8dadc,stroke:#457b9d,stroke-width:2px
    style IS fill:#a8dadc,stroke:#457b9d,stroke-width:2px
    style PS fill:#a8dadc,stroke:#457b9d,stroke-width:2px
    style NS fill:#a8dadc,stroke:#457b9d,stroke-width:2px
    style AS fill:#a8dadc,stroke:#457b9d,stroke-width:2px
    style SS fill:#a8dadc,stroke:#457b9d,stroke-width:2px
    style GRAF fill:#f4a261,stroke:#e76f51,stroke-width:2px
    style PROM fill:#f4a261,stroke:#e76f51,stroke-width:2px
