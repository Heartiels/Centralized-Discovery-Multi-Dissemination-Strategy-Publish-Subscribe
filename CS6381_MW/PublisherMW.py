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
        self.zk = None  # ZooKeeper client
        self.req = None  # REQ socket to Discovery service
        self.pub = None  # PUB socket for disseminating messages
        self.poller = None
        self.addr = None
        self.port = None
        self.upcall_obj = None
        self.handle_events = True
        self.dissemination = None  # "Direct" or "Broker"
        self.context = None
        self.primary_discovery = None  # Current primary discovery address

    def configure(self, args, zk_client):
        try:
            self.logger.info("PublisherMW::configure")

            # Store ZooKeeper client reference
            self.zk = zk_client

            self.port = args.port
            self.addr = args.addr

            self.group_id = args.group if hasattr(args, "group") else "0"

            config = configparser.ConfigParser()
            config.read(args.config)
            self.dissemination = config["Dissemination"]["Strategy"]

            self.context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = self.context.socket(zmq.REQ)
            self.pub = self.context.socket(zmq.PUB)
            self.poller.register(self.req, zmq.POLLIN)

            # connect_str = "tcp://" + args.discovery
            # self.req.connect(connect_str)

            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)

            self.setup_discovery_watch()
            self.logger.info("PublisherMW::configure completed")
        except Exception as e:
            self.logger.error(f"PublisherMW::configure error: {e}")
            raise e

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

            # 重新注册 Publisher
            if self.upcall_obj:
                self.logger.info("Re-registering with new Discovery leader")
                self.register(self.upcall_obj.name, self.upcall_obj.topiclist)

        except Exception as e:
            self.logger.error(f"Failed to update discovery connection: {e}", exc_info=True)

    def event_loop(self, timeout=1000):
        try:
            self.logger.info("PublisherMW::event_loop - starting event loop")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                if self.req in events:
                    self.handle_discovery_response()
                else:
                    # Handle timeout by invoking upcall
                    if self.upcall_obj:
                        new_timeout = self.upcall_obj.invoke_operation()
                        timeout = new_timeout if new_timeout is not None else 1000

            self.logger.info("PublisherMW::event_loop - exiting event loop")
        except Exception as e:
            self.logger.error(f"PublisherMW::event_loop error: {e}")
            raise e
        finally:
            self.cleanup()

    def handle_discovery_response(self):
        """Process responses from Discovery service"""
        try:
            self.logger.info("PublisherMW::handle_discovery_reply")
            response = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(response)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.debug("Received registration response")
                self.upcall_obj.register_response(disc_resp.register_resp)
            else:
                self.logger.warning(f"Unexpected message type: {disc_resp.msg_type}")

        except Exception as e:
            self.logger.error(f"Response handling failed: {e}")
            raise

    # def handle_reply(self):
    #     try:
    #         self.logger.info("PublisherMW::handle_reply")
    #         bytes_rcvd = self.req.recv()
    #         disc_resp = discovery_pb2.DiscoveryResp()
    #         disc_resp.ParseFromString(bytes_rcvd)
    #         if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
    #             return self.upcall_obj.register_response(disc_resp.register_resp)
    #         elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
    #             return self.upcall_obj.isready_response(disc_resp.isready_resp)
    #         else:
    #             raise ValueError("Unrecognized response message in PublisherMW")
    #     except Exception as e:
    #         self.logger.error(f"PublisherMW::handle_reply error: {e}")
    #         raise e

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
            self.logger.debug(f"PublisherMW::register - Sent registration request: {buf2send}")

            # 等待 Discovery 响应，确保 Publisher 进入 DISSEMINATE
            response = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(response)
            self.logger.debug(f"PublisherMW::register: receiving{response}")

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                if disc_resp.register_resp.status == discovery_pb2.STATUS_SUCCESS:
                    self.logger.info("PublisherMW::register - Registration successful, notifying application")
                    self.upcall_obj.register_response(disc_resp.register_resp)  # 通知应用层
                else:
                    self.logger.warning("PublisherMW::register - Registration failed, retrying...")
                    time.sleep(1)
                    self.register(name, topiclist)  # 失败后重试
            else:
                self.logger.warning("PublisherMW::register - Unexpected response type")

        except Exception as e:
            self.logger.error(f"PublisherMW::register error: {e}")
            raise e

    # def is_ready(self):
    #     try:
    #         self.logger.info("PublisherMW::is_ready")
    #         isready_req = discovery_pb2.IsReadyReq()
    #         disc_req = discovery_pb2.DiscoveryReq()
    #         disc_req.msg_type = discovery_pb2.TYPE_ISREADY
    #         disc_req.isready_req.CopyFrom(isready_req)
    #         buf2send = disc_req.SerializeToString()
    #         self.logger.debug("PublisherMW::is_ready - sending readiness check")
    #         self.req.send(buf2send)
    #     except Exception as e:
    #         self.logger.error(f"PublisherMW::is_ready error: {e}")
    #         raise e

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

    def cleanup(self):
        """Resource cleanup"""
        self.logger.info("Cleaning up resources")
        try:
            self.pub.close()
            self.req.close()
            self.context.term()
            if self.zk:
                self.zk.stop()
                self.zk.close()
        except Exception as e:
            self.logger.warning(f"Cleanup error: {e}")

    def get_primary_discovery(self, zk_client):
        """Retrieve current primary discovery node"""
        try:
            if zk_client.exists("/discovery/leader"):
                data, _ = zk_client.get("/discovery/leader")
                return data.decode()
        except Exception:
            self.logger.warning("Discovery node not found")
