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


import os
import zmq
import time
import json
import logging
import configparser
from CS6381_MW import discovery_pb2

import json
from kazoo.client import KazooClient  # ZooKeeper 客户端
from kazoo.exceptions import NoNodeError, ConnectionClosedError


class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.sub = None  # SUB socket to receive messages
        self.req = None  # REQ socket to talk to Discovery service
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

        # 新增：用于 ZooKeeper 集成
        self.zk = None  # 保存 ZooKeeper 客户端
        self.primary_discovery = None  # 当前连接的 Discovery Leader 地址

        # 新增：标记注册请求是否挂起
        self.registration_pending = False

    def configure(self, args, zk_client):
        try:
            self.logger.debug("SubscriberMW::configure")
            self.port = int(args.port)
            self.addr = args.addr
            self.group_id = args.group if hasattr(args, "group") else "0"
            self.toggle = args.toggle
            self.filename = args.filename
            self.deadline = int(args.deadline)

            config = configparser.ConfigParser()
            config.read(args.config)
            self.dissemination = config["Dissemination"]["Strategy"]

            self.logger.debug("SubscriberMW::configure - setting up ZMQ context")
            self.context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = self.context.socket(zmq.REQ)
            self.sub = self.context.socket(zmq.SUB)
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            # connect_str = f"tcp://{args.discovery}"
            # self.req.connect(connect_str)

            # 保存 ZooKeeper 客户端引用
            self.zk = zk_client
            self.setup_discovery_watch()

        except Exception as e:
            self.logger.error(f"SubscriberMW::configure error: {e}")
            raise e

    def register(self, name, topiclist):
        try:

            if self.registration_pending:
                self.logger.warning("Registration already pending, skipping new registration request")
                return

            self.registration_pending = True  # 设置挂起标志

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

    # def is_ready(self):
    #     try:
    #         self.logger.debug("SubscriberMW::is_ready")
    #         isready_msg = discovery_pb2.IsReadyReq()
    #         disc_req = discovery_pb2.DiscoveryReq()
    #         disc_req.msg_type = discovery_pb2.TYPE_ISREADY
    #         disc_req.isready_req.CopyFrom(isready_msg)
    #         buf2send = disc_req.SerializeToString()
    #         self.req.send(buf2send)
    #         self.logger.debug("SubscriberMW::is_ready - sent readiness check")
    #     except Exception as e:
    #         self.logger.error(f"SubscriberMW::is_ready error: {e}")
    #         raise e

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
                timeout = self.handle_discovery_response()
                if timeout is None:
                    timeout = 1000
            elif self.sub in events:
                message = self.sub.recv_string()
                self.process_message(message)
            else:
                self.logger.error("Unknown event in SubscriberMW::event_loop")
        self.logger.info("SubscriberMW::event_loop - exiting event loop")

    # def handle_reply(self):
    #     try:
    #         self.logger.info("SubscriberMW::handle_reply")
    #         bytes_rcvd = self.req.recv()
    #         disc_resp = discovery_pb2.DiscoveryResp()
    #         disc_resp.ParseFromString(bytes_rcvd)
    #         if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
    #             return self.upcall_obj.register_response(disc_resp.register_resp)
    #         elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
    #             return self.upcall_obj.isready_response(disc_resp.isready_resp)
    #         elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
    #             return self.upcall_obj.lookup_response(disc_resp.lookup_resp)
    #         else:
    #             raise ValueError("Unknown message type in SubscriberMW::handle_reply")
    #     except Exception as e:
    #         self.logger.error(f"SubscriberMW::handle_reply error: {e}")
    #         raise e
    def handle_discovery_response(self):
        """Process responses from Discovery service"""
        try:
            self.logger.info("SubscriberMW::handle_discovery_response")


            response = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(response)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.debug("Received registration response")
                self.upcall_obj.register_response(disc_resp.register_resp)
                self.registration_pending = False
                self.logger.info(f"Received discovery response, msg_type: {disc_resp.msg_type}")
                return 0
            # elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
            #     self.upcall_obj.isready_response(disc_resp.isready_resp)
            #     return 0
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info(f"Received lookup response: {disc_resp.lookup_resp}")
                self.upcall_obj.lookup_response(disc_resp.lookup_resp)
                return 0
            else:
                self.logger.warning(f"Unexpected message type: {disc_resp.msg_type}")
                return 0
        except Exception as e:
            self.logger.error(f"Response handling failed: {e}")
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

            if latency > self.deadline:
                self.logger.error(f"Deadline miss for topic {topic}: latency {latency:.2f} ms exceeds deadline {self.deadline} ms")
                try:
                    node_path = f"/deadline_miss/group_{self.group_id}"
                    if not self.zk.exists(node_path):
                        self.zk.create(node_path, value=f"Deadline miss on topic {topic}: {latency:.2f} ms".encode(), ephemeral=True, makepath=True)
                    else:
                        self.zk.set(node_path, f"Deadline miss on topic {topic}: {latency:.2f} ms".encode())
                except Exception as e:
                    self.logger.error(f"Failed to update deadline_miss node: {e}")

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

        #     if self.primary_discovery:
        #         self.logger.info(f"Disconnecting from old primary: {self.primary_discovery}")
        #         self.req.disconnect(f"tcp://{self.primary_discovery}")
        #     self.primary_discovery = new_primary
        #     self.req.connect(f"tcp://{new_primary}")
        #     self.logger.info(f"Connected to new primary: {new_primary}")
        #     # 如果上层应用对象存在，则尝试重新注册
        #     if self.upcall_obj:
        #         self.logger.info("Re-registering with new Discovery leader")
        #         self.register(self.upcall_obj.name, self.upcall_obj.topiclist)
        # except Exception as e:
        #     self.logger.error(f"Failed to update discovery connection: {e}")
        #     raise


