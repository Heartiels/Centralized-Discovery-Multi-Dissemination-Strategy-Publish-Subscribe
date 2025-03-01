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

        # 新增：用于 ZooKeeper 集成 
        self.zk = None            # 保存 ZooKeeper 客户端
        self.primary_discovery = None  # 当前连接的 Discovery Leader 地址

        # 新增：标记注册请求是否挂起
        self.registration_pending = False

    def configure(self, args, zk_client):
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
            self.logger.info(f"Raw response: {response}")

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
        """监听 Discovery 主节点的变化，并更新连接"""
        def update_primary_discovery(data, stat):
            if data:
                new_primary = data.decode()
                self.logger.info(f"Discovery primary changed to {new_primary}")
                self.update_discovery_connection(new_primary)
        # 立即获取当前 Discovery 主节点，并注册 DataWatch
        data, stat = self.zk.get("/discovery/leader", watch=update_primary_discovery)
        update_primary_discovery(data, stat)

    def update_discovery_connection(self, new_primary):
        """更新与新 Discovery 主节点的连接"""
        
        try:
            # 如果之前已经连接了旧的 Discovery Leader
            if self.primary_discovery:
                old_conn_str = f"tcp://{self.primary_discovery}"
                self.logger.info("Waiting briefly for previous request to complete...")
                time.sleep(0.5)
                try:
                    # 尝试清理上一次未完成的响应
                    self.req.setsockopt(zmq.LINGER, 0)
                    self.req.recv(flags=zmq.NOBLOCK)
                except zmq.Again:
                    pass
                # 断开旧连接，并关闭旧套接字
                self.req.disconnect(old_conn_str)
                self.req.close()
                # 创建新的 REQ 套接字
                self.req = self.context.socket(zmq.REQ)
                self.poller.register(self.req, zmq.POLLIN)
            # 更新新的 leader 地址，并建立连接
            self.primary_discovery = new_primary
            new_conn_str = f"tcp://{self.primary_discovery}"
            self.req.connect(new_conn_str)
            self.logger.info(f"Connected to new primary: {new_primary}")
        except Exception as e:
            self.logger.error(f"更新 discovery 连接失败: {e}")

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
    
    
        