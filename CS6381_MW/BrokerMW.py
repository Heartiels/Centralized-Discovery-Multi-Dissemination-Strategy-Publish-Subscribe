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

        self.context = None
        self.handle_events = True
        self.pub_enabled = False

        self.zk = None
        self.primary_discovery = None
        self.group_id = "0"

    def configure(self, args, zk_client):
        try:
            self.logger.info("BrokerMW::configure")

            self.zk = zk_client
            self.addr = args.addr
            self.port = int(args.port)

            self.group_id = args.group if hasattr(args, "group") else "0"

            self.context = zmq.Context()
            self.req = self.context.socket(zmq.REQ)
            self.poller = zmq.Poller()
            self.poller.register(self.req, zmq.POLLIN)

            # discovery_host_port = args.discovery
            # connect_str = f"tcp://{discovery_host_port}"

            # self.logger.info(f"BrokerMW::configure - connecting REQ socket to {connect_str}")
            # self.req.connect(connect_str)
            self.setup_discovery_watch()

            self.logger.info("BrokerMW::configure completed")
        except Exception as e:
            self.logger.error(f"BrokerMW::configure error: {e}", exc_info=True)
            raise e
    
    def enable_pubsub(self):
        if not self.pub_enabled:
            self.logger.info("BrokerMW::enable_pubsub - enabling PUB/SUB sockets")

            self.pub = self.context.socket(zmq.PUB)
            pub_bind_str = f"tcp://*:{self.port}"
            self.pub.bind(pub_bind_str)

            self.sub = self.context.socket(zmq.SUB)
            self.poller.register(self.sub, zmq.POLLIN)

            self.pub_enabled = True

            self.logger.info("BrokerMW::enable_pubsub - registering as ROLE_BOTH")
            self.register(self.upcall_obj.name, [])

    def disable_pubsub(self):
        if self.pub_enabled:
            self.logger.info("BrokerMW::disable_pubsub - closing PUB/SUB sockets")

            self.poller.unregister(self.sub)
            self.sub.close()
            self.pub.close()

            self.pub_enabled = False

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

            buf2send = disc_req.SerializeToString()
            self.logger.debug("BrokerMW::register - sending registration request")
            self.req.send(buf2send)

        except Exception as e:
            self.logger.error(f"BrokerMW::register error: {e}", exc_info=True)
            raise e

    def request_pubs(self):
        try:
            self.logger.info("BrokerMW::request_pubs")
            lookup_req = discovery_pb2.LookupPubByTopicReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
            disc_req.lookup_req.CopyFrom(lookup_req)
            self.req.send(disc_req.SerializeToString())
        except Exception as e:
            self.logger.error(f"BrokerMW::request_pubs error: {e}", exc_info=True)
            raise e
    
    def is_ready(self):
        try:
            self.logger.info("BrokerMW::is_ready")
            isready_req = discovery_pb2.IsReadyReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)
            self.req.send(disc_req.SerializeToString())
        except Exception as e:
            self.logger.error(f"BrokerMW::is_ready error: {e}", exc_info=True)
            raise e


    def event_loop(self, timeout=None):
        try:
            self.logger.info("BrokerMW::event_loop - starting event loop")
            while self.handle_events:
                if timeout is None:
                    timeout = 1000

                events = dict(self.poller.poll(timeout=timeout))
                if not events:
                    new_timeout = self.upcall_obj.invoke_operation()
                    timeout = new_timeout if new_timeout is not None else 1000
                else:
                    if self.req in events:
                        self.handle_reply()

                    if self.pub_enabled and self.sub and (self.sub in events):
                        message = self.sub.recv_string()
                        self.logger.debug(f"BrokerMW::event_loop - received from pub: {message}")
                        self.pub.send_string(message)

            self.logger.info("BrokerMW::event_loop - exiting event loop")
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop error: {e}", exc_info=True)
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
                self.logger.error("Unrecognized response message from Discovery")
                raise ValueError("Unrecognized response message")
        except Exception as e:
            self.logger.error(f"BrokerMW::handle_reply error: {e}", exc_info=True)
            raise e
        
    def lookup_bind(self, addr, port):
        try:
            if self.pub_enabled and self.sub:
                connect_str = f"tcp://{addr}:{port}"
                self.logger.info(f"BrokerMW::lookup_bind - SUB connect to {connect_str}")
                self.sub.connect(connect_str)
                self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
        except Exception as e:
            self.logger.error(f"BrokerMW::lookup_bind error: {e}", exc_info=True)
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
        self.logger.info("BrokerMW::disable_event_loop - event loop disabled")

    def cleanup(self):
        try:
            if self.sub:
                self.sub.close()
            if self.pub:
                self.pub.close()
            if self.req:
                self.req.close()
            if self.context:
                self.context.term()
        except Exception as e:
            self.logger.warning(f"BrokerMW::cleanup error: {e}")


    def setup_discovery_watch(self):
        """监听 Discovery 主节点的变化"""

        def update_primary_discovery(data, event=None):
            """ 当 ZooKeeper 发现 /discovery/leader 发生变化时更新 """
            if data:
                new_primary = data.decode() if isinstance(data, bytes) else data
                self.logger.info(f"Discovery primary changed to {new_primary}")
                self.update_discovery_connection(new_primary)
            else:
                self.logger.warning("Leader node is empty. Waiting for a new leader...")

        # 立即获取当前 Discovery
        watch_path = f"/discovery/leader/group_{self.group_id}"
        self.logger.info(f"Setting up DataWatch for {watch_path}")
        self.zk.DataWatch(watch_path, update_primary_discovery)

    def update_discovery_connection(self, new_primary):
        """发现新 Leader 并重新注册"""
        try:
            if self.primary_discovery == new_primary:
                return  # 避免重复连接

            if self.primary_discovery:
                self.logger.info(f"Disconnecting from old primary: {self.primary_discovery}")
                self.req.disconnect(f"tcp://{self.primary_discovery}")

            self.primary_discovery = new_primary
            discovery_host, discovery_port = new_primary.split(":")
            discovery_addr = f"tcp://{discovery_host}:{discovery_port}"

            self.logger.info(f"Connecting to new primary Discovery: {discovery_addr}")
            self.req.connect(discovery_addr)

            # 重新注册
            if self.upcall_obj:
                self.logger.info("Re-registering with new Discovery leader")
                self.register(self.upcall_obj.name, self.upcall_obj.topiclist)

        except Exception as e:
            self.logger.error(f"Failed to update discovery connection: {e}", exc_info=True)