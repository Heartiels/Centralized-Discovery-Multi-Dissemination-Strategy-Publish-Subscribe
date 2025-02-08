h1 python3 DiscoveryAppln.py -P 4 -S 4 -p 5555 -c config.ini -l 10 >dis.out 2>&1 &
h2 python3 BrokerAppln.py -n broker -a "10.0.0.2" -p 5570 -d "10.0.0.1:5555" -T 5 -c config.ini -l 10 >broker.out 2>&1 &
h3 python3 PublisherAppln.py -n pub1 -a "10.0.0.3" -p 5571 -d "10.0.0.1:5555" -T 3 -f 1 -i 100 -c config.ini -l 10 >pub1.out 2>&1 &
h4 python3 PublisherAppln.py -n pub2 -a "10.0.0.4" -p 5572 -d "10.0.0.1:5555" -T 3 -f 1 -i 100 -c config.ini -l 10 >pub2.out 2>&1 &
h5 python3 PublisherAppln.py -n pub3 -a "10.0.0.5" -p 5573 -d "10.0.0.1:5555" -T 3 -f 1 -i 100 -c config.ini -l 10 >pub3.out 2>&1 &
h6 python3 PublisherAppln.py -n pub4 -a "10.0.0.6" -p 5574 -d "10.0.0.1:5555" -T 3 -f 1 -i 100 -c config.ini -l 10 >pub4.out 2>&1 &
h7 python3 SubscriberAppln.py -n sub1 -a "10.0.0.7" -p 5560 -d "10.0.0.1:5555" -T 3 -c config.ini -l 10 -t -f latency.json >sub1.out 2>&1 &
h8 python3 SubscriberAppln.py -n sub2 -a "10.0.0.8" -p 5561 -d "10.0.0.1:5555" -T 3 -c config.ini -l 10 -t -f latency.json >sub2.out 2>&1 &
h9 python3 SubscriberAppln.py -n sub3 -a "10.0.0.9" -p 5562 -d "10.0.0.1:5555" -T 3 -c config.ini -l 10 -t -f latency.json >sub3.out 2>&1 &
h10 python3 SubscriberAppln.py -n sub4 -a "10.0.0.10" -p 5563 -d "10.0.0.1:5555" -T 3 -c config.ini -l 10 -t -f latency.json >sub4.out 2>&1 &