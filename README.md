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

---

### 5. Running the System
5.1 Direct Dissemination Mode (with ZooKeeper and group-based load balancing)
1. Start the Discovery Service (e.g., group 0):
<pre lang="markdown"> python3 DiscoveryAppln.py --group 0 -p 5555 -c config.ini -l 10
</pre>
2. Start Publishers (specify the same group):
<pre lang="markdown"> python3 PublisherAppln.py --group 0 -n pub1 -a localhost -p 5577 -T 3 -f 1 -i 100 -c config.ini -l 10
python3 PublisherAppln.py --group 0 -n pub2 -a localhost -p 5578 -T 3 -f 1 -i 100 -c config.ini -l 10
</pre>
3. Start Subscribers (same group):
<pre lang="markdown"> python3 SubscriberAppln.py --group 0 -n sub1 -a localhost -p 5560 -T 3 -c config.ini -l 10 -t -f latency.json
python3 SubscriberAppln.py --group 0 -n sub2 -a localhost -p 5561 -T 3 -c config.ini -l 10 -t -f latency2.json
</pre>

5.2 ViaBroker Dissemination Mode (with ZooKeeper and group-based load balancing)

1. Start the Discovery Service:
<pre lang="markdown"> python3 DiscoveryAppln.py --group 0 -p 5555 -c config.ini -l 10
</pre>

2. Start the Broker:
<pre lang="markdown"> python3 BrokerAppln.py --group 0 -n broker -a localhost -p 5570 -T 3 -c config.ini -l 10
</pre>

3. Start Publishers:
<pre lang="markdown"> python3 PublisherAppln.py --group 0 -n pub1 -a localhost -p 5577 -T 3 -f 1 -i 100 -c config.ini -l 10
python3 PublisherAppln.py --group 0 -n pub2 -a localhost -p 5578 -T 3 -f 1 -i 100 -c config.ini -l 10
</pre>
4. Start Subscribers:
<pre lang="markdown"> python3 SubscriberAppln.py --group 0 -n sub1 -a localhost -p 5560 -T 3 -c config.ini -l 10 -t -f latency.json
python3 SubscriberAppln.py --group 0 -n sub2 -a localhost -p 5561 -T 3 -c config.ini -l 10 -t -f latency2.json
</pre>
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
















