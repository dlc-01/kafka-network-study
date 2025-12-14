# Kafka Broker (Educational)

An educational implementation of a Kafka-compatible broker built from scratch.  
The project focuses on understanding Kafka internals by implementing the wire protocol, metadata handling, and basic producer/consumer workflows.

This is **not** a production-ready Kafka broker.

---

## Implemented Features

- TCP server with concurrent client support
- Kafka wire protocol parsing and response building
- ApiVersions handling
- DescribeTopicPartitions support
- Fetch (consume messages from disk)
- Produce (append messages to disk)
- Metadata loading from log-based storage
- Correct Correlation ID handling

---

## Supported Scenarios

### ApiVersions
- Parse and validate API versions

### DescribeTopicPartitions
- Unknown topic
- Single and multiple partitions
- Multiple topics

### Fetch
- No topics
- Unknown topic
- Empty topic
- Single and multiple messages

### Produce
- Invalid topic or partition
- Single and multiple records
- Multiple partitions and topics

