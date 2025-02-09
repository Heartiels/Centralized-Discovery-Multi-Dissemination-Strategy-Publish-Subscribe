###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher middleware code
#
# Created: Spring 2023
#
###############################################

# The publisher middleware does not do anything on its own. It must be used
# by the application level logic. This middleware object maintains the ZMQ
# sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the PUB role to disseminate topics
# (2) It must, on behalf of the application logic, register the publisher application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the publisher appln, it must also query the discovery service (when instructed) 
# to see if it is fine to start dissemination
# (4) It must do the actual dissemination activity of the topic data when instructed by the 

import os
import sys
import time
import logging
import zmq
import configparser
from CS6381_MW import discovery_pb2

class PublisherMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # REQ socket to Discovery service
        self.pub = None  # PUB socket for disseminating messages
        self.poller = None
        self.addr = None
        self.port = None
        self.upcall_obj = None
        self.handle_events = True
        self.dissemination = None  # "Direct" or "Broker"

    def configure(self, args):
        try:
            self.logger.info("PublisherMW::configure")
            self.port = args.port
            self.addr = args.addr

            config = configparser.ConfigParser()
            config.read(args.config)
            self.dissemination = config["Dissemination"]["Strategy"]

            context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.poller.register(self.req, zmq.POLLIN)

            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)

            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)
            self.logger.info("PublisherMW::configure completed")
        except Exception as e:
            self.logger.error(f"PublisherMW::configure error: {e}")
            raise e

    def event_loop(self, timeout=1000):
        try:
            self.logger.info("PublisherMW::event_loop - starting event loop")
            while self.handle_events:
                if timeout is None:
                    timeout = 1000
                events = dict(self.poller.poll(timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                    if timeout is None:
                        timeout = 1000
                elif self.req in events:
                    timeout = self.handle_reply()
                    if timeout is None:
                        timeout = 1000
                else:
                    raise Exception("Unknown event in PublisherMW::event_loop")
            self.logger.info("PublisherMW::event_loop - exiting event loop")
        except Exception as e:
            self.logger.error(f"PublisherMW::event_loop error: {e}")
            raise e

    def handle_reply(self):
        try:
            self.logger.info("PublisherMW::handle_reply")
            bytes_rcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_rcvd)
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                return self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                return self.upcall_obj.isready_response(disc_resp.isready_resp)
            else:
                raise ValueError("Unrecognized response message in PublisherMW")
        except Exception as e:
            self.logger.error(f"PublisherMW::handle_reply error: {e}")
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info("PublisherMW::register")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port

            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_PUBLISHER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist.extend(topiclist)

            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            buf2send = disc_req.SerializeToString()
            self.logger.debug("PublisherMW::register - sending registration request")
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error(f"PublisherMW::register error: {e}")
            raise e

    def is_ready(self):
        try:
            self.logger.info("PublisherMW::is_ready")
            isready_req = discovery_pb2.IsReadyReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)
            buf2send = disc_req.SerializeToString()
            self.logger.debug("PublisherMW::is_ready - sending readiness check")
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error(f"PublisherMW::is_ready error: {e}")
            raise e

    def disseminate(self, id, topic, data):
        try:
            self.logger.debug("PublisherMW::disseminate")
            send_str = topic + ":" + data + ":" + str(time.time())
            self.logger.debug(f"PublisherMW::disseminate - sending: {send_str}")
            self.pub.send(bytes(send_str, "utf-8"))
            self.logger.debug("PublisherMW::disseminate complete")
        except Exception as e:
            self.logger.error(f"PublisherMW::disseminate error: {e}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False