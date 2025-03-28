# CS 6381: Distributed Systems Principles – Programming Assignment 3

**Spring 2025**

---

### 1. Overview
This project extends the previous publish–subscribe middleware by incorporating **load balancing**, **fault tolerance**, and a foundation for **QoS (Quality of Service)** features. This is achieved using **ZooKeeper** to manage multiple **load-balanced groups** of Discovery and Broker services. Each group maintains its own leader election and registration paths. In **Milestone 1**, we implemented static group-based load balancing, where clients (publishers and subscribers) can specify which group to connect to via a `--group` parameter.

---

### 2. Project Architecture
The base architecture remains unchanged from Assignment 2. Assignment 3 introduces **group-based logical partitioning**:

- Each Discovery or Broker instance is part of a **load balancing group**, such as `group_0`, `group_1`, etc.
- ZooKeeper maintains separate znodes for each group (e.g., `/discovery/leader/group_0`, `/brokers/leader/group_0`).
- Publishers and Subscribers specify `--group` to connect to the appropriate group’s Discovery and Broker services.
- Leader election and service registration are isolated within each group, providing **scalability** and **fault isolation**.

---

## File Structure


<pre lang="markdown"> ```text . ├── CS6381_MW/ │ ├── __init__.py # Package initializer │ ├── BrokerMW.py # Updated with load balancing (group) support │ ├── Common.py # Shared constants and utilities │ ├── discovery_pb2.py # gRPC generated code │ ├── discovery.proto # Protocol buffer definition for Discovery │ ├── DiscoveryMW.py # Middleware with group support for Discovery │ ├── PublisherMW.py # Middleware for Publishers with --group support │ ├── SubscriberMW.py # Middleware for Subscribers with --group support │ └── topic.proto # Protocol buffer for topic communication │ ├── EXPERIMENTS/ │ ├── local_5P_4S.sh # Script for local testing with 5 Publishers and 4 Subscribers │ ├── Local_README # Instructions for local test setup │ ├── mininet_1S_10H_5P_4S.sh # Mininet test script │ └── Mininet_README # Instructions for Mininet setup │ ├── config.ini # Configuration for Discovery, Dissemination, ZooKeeper, etc. ├── BrokerAppln.py # Application logic for Broker (supports group-based load balancing) ├── DiscoveryAppln.py # Discovery service logic with group-based leader election ├── KeyChanges.docx # Summary of key changes for Assignment 3 ├── PublisherAppln.py # Publisher application (now supports --group) ├── README.md # Project documentation ├── SubscriberAppln.py # Subscriber application (now supports --group) ├── TESTING/ # Test cases and validation scripts └── topic_selector.py # Topic selection utility ``` </pre>


---

### 4. Environment Setup and Dependencies
**Prerequisites:**
- Python 3.6 or higher
- [ZeroMQ](http://zeromq.org/) & [pyzmq](https://pypi.org/project/pyzmq/)
- [Protobuf](https://developers.google.com/protocol-buffers)
- [Apache ZooKeeper](https://zookeeper.apache.org/) (ensure a single ZooKeeper instance is running)

---

### 5. Running the System
5.1 Direct Dissemination Mode (with ZooKeeper and group-based load balancing)

1. Start the Discovery Service (e.g., group 0):
python3 DiscoveryAppln.py --group 0 -p 5555 -c config.ini -l 10

2. Start Publishers (specify the same group):
python3 PublisherAppln.py --group 0 -n pub1 -a localhost -p 5577 -T 3 -f 1 -i 100 -c config.ini -l 10
python3 PublisherAppln.py --group 0 -n pub2 -a localhost -p 5578 -T 3 -f 1 -i 100 -c config.ini -l 10

3. Start Subscribers (same group):
python3 SubscriberAppln.py --group 0 -n sub1 -a localhost -p 5560 -T 3 -c config.ini -l 10 -t -f latency.json
python3 SubscriberAppln.py --group 0 -n sub2 -a localhost -p 5561 -T 3 -c config.ini -l 10 -t -f latency2.json

5.2 ViaBroker Dissemination Mode (with ZooKeeper and group-based load balancing)

1. Start the Discovery Service:
python3 DiscoveryAppln.py --group 0 -p 5555 -c config.ini -l 10

2. Start the Broker:
python3 BrokerAppln.py --group 0 -n broker -a localhost -p 5570 -T 3 -c config.ini -l 10

3. Start Publishers:
python3 PublisherAppln.py --group 0 -n pub1 -a localhost -p 5577 -T 3 -f 1 -i 100 -c config.ini -l 10
python3 PublisherAppln.py --group 0 -n pub2 -a localhost -p 5578 -T 3 -f 1 -i 100 -c config.ini -l 10

4. Start Subscribers:
python3 SubscriberAppln.py --group 0 -n sub1 -a localhost -p 5560 -T 3 -c config.ini -l 10 -t -f latency.json
python3 SubscriberAppln.py --group 0 -n sub2 -a localhost -p 5561 -T 3 -c config.ini -l 10 -t -f latency2.json

Note: Each service instance (Discovery, Broker, Publisher, Subscriber) uses the same --group value to operate within the same ZooKeeper partition. You can create multiple groups (e.g., group 1, group 2) by launching additional instances with different --group values.

---

### 6. Reuslts
Latency testing in Milestone 1 shows that the added ZooKeeper-based group logic does not introduce significant overhead.
Most message latencies remain within 1–2ms, with occasional spikes due to expected network variance. The system remains responsive and scalable under multiple logical groups.
![image](https://github.com/Heartiels/Centralized-Discovery-Multi-Dissemination-Strategy-Publish-Subscribe/blob/main/latency_results/result.png)

## 7. Demonstration Video
Programming Assignment 1: https://youtu.be/rihmkwGVek8

Programming Assignment 2: https://youtu.be/9kSFTGLd9-c

Programming Assignment 3: 

Authors
Haowen Yao
Xindong Zheng
Yan Zhang
















