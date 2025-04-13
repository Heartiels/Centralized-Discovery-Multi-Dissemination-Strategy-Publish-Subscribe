# CS 6381: Distributed Systems Principles – Programming Assignment 4

**Spring 2025**

---

### 1. Overview
This project extends the previous publish–subscribe middleware by incorporating advanced **Quality of Service (QoS)** features in addition to load balancing and fault tolerance. In previous assignments, we focused on fault tolerance (PA2) and load balancing (PA3). In PA4, we extend the system with three types of QoS:

- **Ownership Strength:**  
  In the case of multiple publishers publishing on the same topic, only the publisher with the highest ownership strength (determined via ZooKeeper ephemeral sequential nodes) is allowed to disseminate data to subscribers. If the highest publisher fails, the next highest takes over.

- **History QoS:**  
  Publishers now maintain a finite history (a sliding window) of the most recent N messages per topic (where N is configurable). Late-joining subscribers can request a certain amount of history; however, the match is only valid if the offered history meets the requested amount (as defined by the “offered vs. requested” model).

- **Deadline QoS:**  
  Each message is timestamped and subscribers compute the end-to-end delay. If the latency exceeds a specified deadline threshold (e.g., 50ms), the subscriber flags a deadline miss. In addition, the system updates a dedicated ZooKeeper node (e.g., `/deadline_miss/group_<group_id>`) to indicate that the current Broker group failed to meet the deadline and to enable load balancing logic for switching if needed.

Together with our earlier load balancing and fault tolerance implementations, these QoS features aim to improve the overall performance and reliability of the system under various load conditions.

---

### 2. Project Architecture
The core architecture remains similar to previous assignments but now includes QoS functionality:

- **Discovery Service**  
  - *DiscoveryAppln.py*: Handles registrations, lookups and integrates QoS information (e.g., offered history).  
  - *DiscoveryMW.py*: Uses ZeroMQ REP sockets to process requests and, during lookup, selects the publisher with the best ownership strength or returns Broker information in Broker mode.

- **Publishers**  
  - *PublisherAppln.py*: Application logic for publishing data on topics. Now, registration includes offered history information (the number of historical samples maintained).  
  - *PublisherMW.py*: Maintains a sliding window of the most recent N messages per topic (History QoS) and timestamps each message. Ownership Strength is also generated during registration using ZooKeeper ephemeral sequential nodes.

- **Subscribers**  
  - *SubscriberAppln.py*: Registers itself with the Discovery Service, specifying a requested history (if needed) and a deadline threshold for message delivery.  
  - *SubscriberMW.py*: Listens for messages and computes end-to-end delay. If the delay exceeds the deadline, logs a deadline miss and updates a ZooKeeper node for monitoring.

- **Broker** (used in the ViaBroker Dissemination strategy)  
  - *BrokerAppln.py*: Acts as an intermediary in Broker mode to receive and forward messages.  
  - *BrokerMW.py*: Uses a mix of PUB, SUB, and REQ sockets and integrates the QoS configuration indirectly via Discovery information.

- **QoS Mode and Load Balancing:**  
  All components also use group-based logical partitioning (via `--group` parameter). ZooKeeper znodes include the group ID (e.g., `/discovery/leader/group_0`, `/brokers/leader/group_0`) ensuring that all entities in the same group work together.

---

### 3. File Structure


<pre lang="markdown">├── CS6381_MW/
│   ├── __init__.py                
│   ├── BrokerMW.py                # Updated with load balancing (group) support
│   ├── Common.py                  
│   ├── discovery_pb2.py           
│   ├── discovery.proto            
│   ├── DiscoveryMW.py             # Updated with group parameter support in configuration/logging
│   ├── PublisherMW.py             # Updated to use --group and listen on /discovery/leader/group_<group_id>
│   ├── SubscriberMW.py            # Updated similarly for --group support
│   └── topic.proto                
│
├── EXPERIMENTS/                   
│   ├── local_5P_4S.sh             
│   ├── Local_README               
│   ├── mininet_1S_10H_5P_4S.sh    
│   └── Mininet_README             
│
├── config.ini                     # Configuration file for Discovery and Dissemination strategies, ZooKeeper hosts, etc.
├── BrokerAppln.py                 # Application logic for the Broker (with load balancing group support)
├── DiscoveryAppln.py              # Application logic for the Discovery Service (with group-based leader election)
├── KeyChanges.docx                # Document detailing key modifications for Assignment 3
├── PublisherAppln.py              # Application logic for Publishers (now accepts --group)
├── README.md                      # Project documentation (this file)
├── SubscriberAppln.py             # Application logic for Subscribers (now accepts --group)
├── TESTING/                       # Test scripts and files
└── topic_selector.py              # Utility for topic selection 
</pre>

---

### 4. Environment Setup and Dependencies
**Prerequisites:**
- Python 3.6 or higher
- [ZeroMQ](http://zeromq.org/) & [pyzmq](https://pypi.org/project/pyzmq/)
- [Protobuf](https://developers.google.com/protocol-buffers)
- [Apache ZooKeeper](https://zookeeper.apache.org/) (ensure a single ZooKeeper instance is running)

**Installation:**
- Install required Python packages:
<pre lang="markdown"> pip3 install pyzmq protobuf kazoo
</pre>
- Configure ZooKeeper in config.ini (e.g., set ZooKeeper hosts to 127.0.0.1:2181).

---

### 5. Running the System
5.1 Direct Dissemination Mode (with ZooKeeper and group-based load balancing)

1. Start the Discovery Service (e.g., group 0):
<pre lang="markdown"> python3 DiscoveryAppln.py --group 0 -p 5555 -c config.ini -l 10
</pre>

2. Start Publishers (specify same group and history offered, e.g., 10):
<pre lang="markdown"> python3 PublisherAppln.py --group 0 --history-offered 10 -n pub1 -a localhost -p 5577 -T 3 -f 1 -i 100 -c config.ini -l 10
 python3 PublisherAppln.py --group 0 --history-offered 10 -n pub2 -a localhost -p 5578 -T 3 -f 1 -i 100 -c config.ini -l 10
</pre>

3. Start Subscribers (specify same group, history request, and a deadline, e.g., 5 and 50ms):
<pre lang="markdown"> python3 SubscriberAppln.py --group 0 --history-request 5 --deadline 50 -n sub1 -a localhost -p 5560 -T 3 -c config.ini -l 10 -t -f latency_sub1.json
 python3 SubscriberAppln.py --group 0 --history-request 5 --deadline 50 -n sub2 -a localhost -p 5561 -T 3 -c config.ini -l 10 -t -f latency_sub2.json
</pre>

5.2 ViaBroker Dissemination Mode (with ZooKeeper and group-based load balancing)

1. Start the Discovery Service:
<pre lang="markdown"> python3 DiscoveryAppln.py --group 0 -p 5555 -c config.ini -l 10
</pre>

2. Start the Broker:
<pre lang="markdown"> python3 BrokerAppln.py --group 0 -n broker -a localhost -p 5570 -T 3 -c config.ini -l 10
</pre>

3. Start Publishers:
<pre lang="markdown"> python3 PublisherAppln.py --group 0 --history-offered 10 -n pub1 -a localhost -p 5577 -T 3 -f 1 -i 100 -c config.ini -l 10
 python3 PublisherAppln.py --group 0 --history-offered 10 -n pub2 -a localhost -p 5578 -T 3 -f 1 -i 100 -c config.ini -l 10
</pre>

4. Start Subscribers:
<pre lang="markdown"> python3 SubscriberAppln.py --group 0 --history-request 5 --deadline 50 -n sub1 -a localhost -p 5560 -T 3 -c config.ini -l 10 -t -f latency_sub1.json
 python3 SubscriberAppln.py --group 0 --history-request 5 --deadline 50 -n sub2 -a localhost -p 5561 -T 3 -c config.ini -l 10 -t -f latency_sub2.json
</pre>

Note: All components must be launched with the same --group value to work within the same ZooKeeper partition. You can launch additional groups by specifying different group IDs.

---

### 6. Experimental Results and Performance Comparison
Based on our experimental observations, we can draw the following preliminary conclusions:
- End-to-End Latency:
Under normal operating conditions, the system's end-to-end latency remains in the range of 1–2ms both in previous assignments (PA2 and PA3) and in our current PA4 implementation. However, when artificial delays are introduced (for example, to simulate heavy load or network slowdowns), the Deadline QoS mechanism in PA4 successfully detects deadline misses, which is not available in PA2/PA3.
  
- Ownership Strength and History QoS:
The introduction of Ownership Strength ensures that only data from the highest-priority publisher (for a given topic) is relayed to subscribers. Additionally, the History QoS mechanism allows late-joining subscribers to retrieve a fixed number of recent messages if the offered history meets their requested threshold. These features improve data relevance and consistency, especially in dynamic scenarios where publishers may join or leave.

- Deadline QoS:
Our experiments indicate that, while the baseline latency remains low, the Deadline QoS functionality can reliably flag instances where message delivery exceeds the specified deadline (e.g., beyond 50ms). This provides a basis for dynamic broker switching or remedial actions, contributing to improved real-time performance under stressed conditions.

In summary, PA4 demonstrates that integrating QoS features (Ownership Strength, History QoS, and Deadline QoS) does not significantly degrade normal performance but provides substantial benefits in detecting and handling overload or failure conditions, thereby enhancing overall system robustness and real-time responsiveness compared to PA2 and PA3.

## 7. Demonstration Video
Programming Assignment 1: https://youtu.be/rihmkwGVek8

Programming Assignment 2: https://youtu.be/9kSFTGLd9-c

Programming Assignment 3: Pictures are enough to show our results.
Latency testing in Milestone 1 shows that the added ZooKeeper-based group logic does not introduce significant overhead.
Most message latencies remain within 1–2ms, with occasional spikes due to expected network variance. The system remains responsive and scalable under multiple logical groups.
![image](https://github.com/Heartiels/Centralized-Discovery-Multi-Dissemination-Strategy-Publish-Subscribe/blob/main/latency_results/result.png)

Programming Assignment 4: https://youtu.be/3j4W5Q1WeGw

## Authors
Haowen Yao
Xindong Zheng
Yan Zhang
















