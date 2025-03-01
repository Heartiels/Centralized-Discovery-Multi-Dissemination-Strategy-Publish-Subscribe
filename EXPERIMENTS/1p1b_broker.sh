h1 python3 DiscoveryAppln.py -P 1 -S 1 -p 5555 -c config.ini -l 10 >dis.out 2>&1 &
h2 python3 BrokerAppln.py -n broker -a "10.0.0.2" -p 5570 -d "10.0.0.1:5555" -T 5 -c config.ini -l 10 >broker.out 2>&1 &
h3 python3 PublisherAppln.py -n pub1 -a "10.0.0.3" -p 5577 -d "10.0.0.1:5555" -T 3 -f 1 -i 100 -c config.ini -l 10 >pub.out 2>&1 &
h4 python3 SubscriberAppln.py -n sub1 -a "10.0.0.4" -p 5560 -d "10.0.0.1:5555" -T 3 -c config.ini -l 10 -t -f latency.json >sub.out 2>&1 &