###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.
import os
import sys
import time
import logging
import zmq
from CS6381_MW import discovery_pb2

class BrokerMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None      # REQ socket to talk to Discovery service
        self.pub = None      # PUB socket to disseminate messages to subscribers
        self.sub = None      # SUB socket to receive messages from publishers
        self.poller = None
        self.addr = None     # Broker's IP address
        self.port = None     # Broker's dissemination port
        self.upcall_obj = None
        self.handle_events = True
        self.topiclist = None

    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")
            self.addr = args.addr
            self.port = int(args.port)

            # Set up ZMQ context and sockets
            self.logger.debug("BrokerMW::configure - obtaining ZMQ context")
            context = zmq.Context()

            self.logger.debug("BrokerMW::configure - creating REQ, PUB, and SUB sockets")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.sub = context.socket(zmq.SUB)

            self.logger.debug("BrokerMW::configure - setting up poller")
            self.poller = zmq.Poller()
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            # Connect REQ socket to Discovery service
            connect_str = f"tcp://{args.discovery}"
            self.logger.debug(f"BrokerMW::configure - connecting REQ socket to {connect_str}")
            self.req.connect(connect_str)

            # Bind PUB socket (for disseminating to subscribers)
            pub_bind_str = f"tcp://*:{self.port}"
            self.logger.debug(f"BrokerMW::configure - binding PUB socket to {pub_bind_str}")
            self.pub.bind(pub_bind_str)

            self.logger.info("BrokerMW::configure completed")
        except Exception as e:
            self.logger.error(f"BrokerMW::configure error: {e}")
            raise e

    def event_loop(self, timeout=None):
        try:
            self.logger.info("BrokerMW::event_loop - starting event loop")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                elif self.sub in events:
                    # Receive message from publishers and forward to subscribers
                    message = self.sub.recv_string()
                    self.logger.debug(f"BrokerMW::event_loop - received message: {message}")
                    self.pub.send_string(message)

            self.logger.info("BrokerMW::event_loop - exiting event loop")
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop error: {e}")
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info("BrokerMW::register")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port

            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_BOTH
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist.extend(topiclist)

            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            # Subscribe to each topic on the SUB socket (to receive from publishers)
            for topic in topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)

            buf2send = disc_req.SerializeToString()
            self.logger.debug("BrokerMW::register - sending registration request")
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error(f"BrokerMW::register error: {e}")
            raise e

    def is_ready(self):
        try:
            self.logger.info("BrokerMW::is_ready")
            isready_req = discovery_pb2.IsReadyReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)
            buf2send = disc_req.SerializeToString()
            self.logger.debug("BrokerMW::is_ready - sending readiness check")
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error(f"BrokerMW::is_ready error: {e}")
            raise e

    def request_pubs(self):
        try:
            self.logger.info("BrokerMW::request_pubs")
            pubs_req = discovery_pb2.LookupPubByTopicReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
            disc_req.lookup_req.CopyFrom(pubs_req)
            buf2send = disc_req.SerializeToString()
            self.logger.debug("BrokerMW::request_pubs - sending request")
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error(f"BrokerMW::request_pubs error: {e}")
            raise e

    def handle_reply(self):
        try:
            self.logger.info("BrokerMW::handle_reply")
            bytes_rcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_rcvd)
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                return self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                return self.upcall_obj.isready_response(disc_resp.isready_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                return self.upcall_obj.pubslookup_response(disc_resp.lookup_resp)
            else:
                raise ValueError("Unrecognized response message")
        except Exception as e:
            self.logger.error(f"BrokerMW::handle_reply error: {e}")
            raise e

    def lookup_bind(self, addr, port):
        try:
            self.logger.debug(f"BrokerMW::lookup_bind - connecting SUB socket to tcp://{addr}:{port}")
            self.sub.connect(f"tcp://{addr}:{port}")
        except Exception as e:
            self.logger.error(f"BrokerMW::lookup_bind error: {e}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False