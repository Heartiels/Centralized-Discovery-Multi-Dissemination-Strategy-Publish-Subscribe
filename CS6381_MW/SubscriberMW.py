###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

import os
import zmq
import time
import json
import logging
import configparser
from CS6381_MW import discovery_pb2

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.sub = None    # SUB socket to receive messages
        self.req = None    # REQ socket to talk to Discovery service
        self.poller = None
        self.addr = None
        self.port = None
        self.upcall_obj = None
        self.handle_events = True

        # For latency logging
        self.toggle = None
        self.logging_dict = {}
        self.filename = None
        self.iters = 0
        self.threshold = 50
        self.dissemination = None  # "Direct" or "Broker"

    def configure(self, args):
        try:
            self.logger.debug("SubscriberMW::configure")
            self.port = int(args.port)
            self.addr = args.addr
            self.toggle = args.toggle
            self.filename = args.filename

            config = configparser.ConfigParser()
            config.read(args.config)
            self.dissemination = config["Dissemination"]["Strategy"]

            self.logger.debug("SubscriberMW::configure - setting up ZMQ context")
            context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)
        except Exception as e:
            self.logger.error(f"SubscriberMW::configure error: {e}")
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info("SubscriberMW::register")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port

            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist.extend(topiclist)

            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            for topic in topiclist:
                self.sub.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))

            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
            self.logger.debug("SubscriberMW::register - sent registration request")
        except Exception as e:
            self.logger.error(f"SubscriberMW::register error: {e}")
            raise e

    def is_ready(self):
        try:
            self.logger.debug("SubscriberMW::is_ready")
            isready_msg = discovery_pb2.IsReadyReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_msg)
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
            self.logger.debug("SubscriberMW::is_ready - sent readiness check")
        except Exception as e:
            self.logger.error(f"SubscriberMW::is_ready error: {e}")
            raise e

    def plz_lookup(self, topiclist):
        try:
            self.logger.info("SubscriberMW::plz_lookup")
            lookup_req = discovery_pb2.LookupPubByTopicReq()
            lookup_req.topiclist.extend(topiclist)
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_req.lookup_req.CopyFrom(lookup_req)
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
            self.logger.debug("SubscriberMW::plz_lookup - sent lookup request")
        except Exception as e:
            self.logger.error(f"SubscriberMW::plz_lookup error: {e}")
            raise e

    def event_loop(self, timeout=1000):
        start_time = time.time()
        self.logger.debug("SubscriberMW::event_loop - starting event loop")
        while self.handle_events:
            if timeout is None:
                timeout = 1000
            events = dict(self.poller.poll(timeout=timeout))
            if not events:
                timeout = self.upcall_obj.invoke_operation()
                if timeout is None:
                    timeout = 1000
                if time.time() - start_time > 100:
                    self.logger.info("No messages received for 100 seconds, saving latency log and exiting.")
                    with open(self.filename, "w") as f:
                        json.dump(self.logging_dict, f, indent=4)
                    break
            elif self.req in events:
                timeout = self.handle_reply()
                if timeout is None:
                    timeout = 1000
            elif self.sub in events:
                message = self.sub.recv_string()
                self.process_message(message)
            else:
                self.logger.error("Unknown event in SubscriberMW::event_loop")
        self.logger.info("SubscriberMW::event_loop - exiting event loop")

    def handle_reply(self):
        try:
            self.logger.info("SubscriberMW::handle_reply")
            bytes_rcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_rcvd)
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                return self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                return self.upcall_obj.isready_response(disc_resp.isready_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                return self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            else:
                raise ValueError("Unknown message type in SubscriberMW::handle_reply")
        except Exception as e:
            self.logger.error(f"SubscriberMW::handle_reply error: {e}")
            raise e

    def process_message(self, message):
        try:
            self.logger.debug(f"Processing received message: {message}")
            parts = message.split(":")
            if len(parts) != 3:
                self.logger.error(f"Malformed message received: {message}")
                return

            topic, content, timestamp = parts
            latency = (time.time() - float(timestamp)) * 1000
            self.logger.info(f"Received topic: {topic}, latency: {latency:.2f} ms")

            if topic not in self.logging_dict:
                self.logging_dict[topic] = [latency]
            else:
                self.logging_dict[topic].append(latency)

            self.iters += 1
            self.logger.debug(f"Message count: {self.iters}, current logging_dict: {self.logging_dict}")
            if self.iters >= self.threshold:
                with open(self.filename, "w") as f:
                    json.dump(self.logging_dict, f, indent=4)
                self.logger.info("SubscriberMW::process_message - saved latency log")
                self.handle_events = False

        except Exception as e:
            self.logger.error(f"Error in process_message: {e}")
            raise e


    def lookup_bind(self, addr, port):
        try:
            self.logger.debug(f"SubscriberMW::lookup_bind - connecting SUB socket to tcp://{addr}:{port}")
            self.sub.connect(f"tcp://{addr}:{port}")
            time.sleep(1)
        except Exception as e:
            self.logger.error(f"SubscriberMW::lookup_bind error: {e}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
        