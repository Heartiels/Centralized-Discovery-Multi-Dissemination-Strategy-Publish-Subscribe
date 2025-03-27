###############################################
#
# Author: Modified by [Your Name]
# Function: Discovery application with ZooKeeper leader election
#
###############################################

import os
import sys
import time
import argparse
import configparser
import logging
import threading
import json

import kazoo
from kazoo.client import KazooClient, KazooState
from kazoo.recipe.election import Election

from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2


class DiscoveryAppln:

    def __init__(self, logger):
        self.pubs = None
        self.subs = None
        self.mw_obj = None
        self.logger = logger
        self.lookup = None
        self.dissemination = None
        self.hm = {}  # Publisher topic mapping
        self.hm2 = {}  # Subscriber topic mapping
        self.cur_pubs = 0
        self.cur_subs = 0
        self.pubset = set()  # Set of publishers

        self.broker_addr = None
        self.broker_port = None

        self.zk = None  # ZooKeeper client
        self.is_leader = False
        self.election_node = None  # 当前选举节点路径

        self.group_id = "0"  # 默认组号为 "0"，后续从命令行参数传入
        self.leader_path = f"/discovery/leader/group_{self.group_id}"
        self.election_path = f"/discovery/election/group_{self.group_id}"


    def configure(self, args):
        ''' Initialize the object '''
        try:
            self.logger.info("DiscoveryAppln::configure")

            # 读取组号参数（可以在命令行中添加 --group 参数）
            self.group_id = args.group if hasattr(args, "group") else "0"
            self.leader_path = f"/discovery/leader/group_{self.group_id}"
            self.election_path = f"/discovery/election/group_{self.group_id}"

            # Reduce Kazoo logging level to only show warnings or higher
            logging.getLogger("kazoo").setLevel(logging.WARNING)

            # Initialize variables
            # self.subs = args.subs
            # self.pubs = args.pubs

            # Read config
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Middleware setup
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args)

            # ZooKeeper setup
            self.zk = KazooClient(hosts=config["ZooKeeper"]["Hosts"])
            self.zk.start()

            # Ensure base paths exist
            self.zk.ensure_path(self.leader_path)
            self.zk.ensure_path(self.election_path)

            # 同时也可以确保注册信息路径存在
            self.zk.ensure_path(f"/registrations/group_{self.group_id}/publishers")
            self.zk.ensure_path(f"/registrations/group_{self.group_id}/subscribers")

            self.watch_leader()
            self.start_election()

        except Exception as e:
            self.logger.error(f"Exception in configure: {e}", exc_info=True)
            raise

    def watch_leader(self):
        """监听 leader 变更，每次变更都输出当前 leader 地址"""
        @self.zk.DataWatch(self.leader_path)
        def leader_watch(data, stat):
            if data:
                leader_addr = data.decode()
                self.logger.info(f"New leader elected: {leader_addr}")
            else:
                self.logger.warning("Leader node deleted. Re-initiating election...")
                self.start_election()

    def start_election(self):
        """加入选举，使用已有节点进行选举"""
        try:
            self.logger.info("Joining election...")

            if self.election_node:
                # 检查节点是否仍然存在
                if self.zk.exists(self.election_node):
                    self.logger.info(f"Reusing existing election node: {self.election_node}")
                    self.check_election_status()
                    return
                else:
                    self.logger.warning(f"Election node {self.election_node} no longer exists, recreating...")

            # 没有可用的 election_node，则创建新节点
            node_value = f"{self.mw_obj.addr}:{self.mw_obj.port}".encode()
            self.election_node = self.zk.create(
                self.election_path + "/node_", node_value, ephemeral=True, sequence=True
            )
            self.logger.info(f"New election node created: {self.election_node}")
            self.check_election_status()

        except Exception as e:
            self.logger.error(f"Error in election process: {e}", exc_info=True)
            raise

    def check_election_status(self):
        """检查选举状态，确定自己是否成为 leader"""
        children = self.zk.get_children(self.election_path)
        children.sort()  # 按照编号排序
        my_node = self.election_node.split("/")[-1]

        if my_node == children[0]:
            self.become_leader()
        else:
            # 监听比自己小的前一个节点
            predecessor = children[children.index(my_node) - 1]
            pred_path = f"{self.election_path}/{predecessor}"
            self.logger.info(f"Watching previous node: {pred_path}")

            @self.zk.DataWatch(pred_path)
            def watch_predecessor(data, stat):
                if data is None:  # 说明前一个节点消失
                    self.logger.info("Predecessor node gone. Checking election status...")
                    self.check_election_status()

    def become_leader(self):
        try:
            self.is_leader = True
            node_value = f"{self.mw_obj.addr}:{self.mw_obj.port}".encode()

            # 更新 leader 节点
            if self.zk.exists(self.leader_path):
                self.zk.delete(self.leader_path)
            self.zk.create(self.leader_path, node_value, ephemeral=True)

            self.logger.info("***** Elected as primary Discovery node *****")

            # 重新加载已注册的 Pub/Sub
            self.reload_registrations()

        except Exception as e:
            self.logger.error(f"Failed to set leader: {e}", exc_info=True)
            raise

    def reload_registrations(self):
        """新 Leader 继承 Publisher 和 Subscriber 的注册信息"""
        try:
            reg_base = f"/registrations/group_{self.group_id}"
            if not self.zk.exists(reg_base):
                self.logger.info("No previous registrations found in ZooKeeper")
                return

            # 重新加载 Publishers
            pub_path = f"{reg_base}/publishers"
            if self.zk.exists(pub_path):
                pubs = self.zk.get_children(pub_path)
                for pub in pubs:
                    data, _ = self.zk.get(f"{pub_path}/{pub}")
                    pub_info = json.loads(data.decode())
                    if not pub_info:
                        continue
                    # 存储在本地缓存
                    for topic in pub_info["topics"]:
                        self.hm.setdefault(topic, []).append(
                            (pub, pub_info["addr"], pub_info["port"])
                        )
                        self.pubset.add((pub, pub_info["addr"], pub_info["port"]))

            # 重新加载 Subscribers
            sub_path = f"{reg_base}/subscribers"
            if self.zk.exists(sub_path):
                subs = self.zk.get_children(sub_path)
                for sub in subs:
                    data, _ = self.zk.get(f"{sub_path}/{sub}")
                    sub_info = json.loads(data.decode())
                    if not sub_info:
                        continue
                    for topic in sub_info["topics"]:
                        self.hm2.setdefault(topic, []).append(
                            (sub, sub_info["addr"], sub_info["port"])
                        )

            self.logger.info("Successfully reloaded existing registrations")

        except Exception as e:
            self.logger.error(f"Failed to reload registrations: {e}", exc_info=True)

    def driver(self):
        ''' 只有主节点才执行事件处理，其他节点等待选举 '''
        try:
            self.logger.info("DiscoveryAppln::driver started")

            self.mw_obj.set_upcall_handle(self)

            while not self.is_leader:
                self.logger.info("Waiting for leader election...")
                time.sleep(2)  # 让非主节点进入等待状态

            self.logger.info("Starting event loop as leader...")
            self.mw_obj.event_loop(timeout=0)  # 仅 leader 执行事件处理

        except Exception as e:
            self.logger.error(f"Exception in driver: {e}", exc_info=True)
            raise

    def register_request(self, register_req):
        try:
            if not self.is_leader:
                # 如果不是 Leader，则返回重定向
                redirect_resp = discovery_pb2.RegisterResp()
                redirect_resp.status = discovery_pb2.STATUS_FAILURE
                primary_data, _ = self.zk.get(self.leader_path)
                redirect_resp.redirect_addr = primary_data.decode()

                discovery_resp = discovery_pb2.DiscoveryResp()
                discovery_resp.msg_type = discovery_pb2.TYPE_REGISTER
                discovery_resp.register_resp.CopyFrom(redirect_resp)
                self.mw_obj.handle_response(discovery_resp)
                return

            # 处理注册
            self.logger.info(
                f"Received registration request from {register_req.info.addr}:{register_req.info.port} "
                f"for topics {register_req.topiclist}"
            )
            role_str = "publishers" if register_req.role == discovery_pb2.ROLE_PUBLISHER else "subscribers"
            path = f"/registrations/group_{self.group_id}/{role_str}/{register_req.info.id}"
            data = json.dumps({
                "addr": register_req.info.addr,
                "port": register_req.info.port,
                "topics": list(register_req.topiclist)
            }).encode()

            if self.zk.exists(path):
                self.zk.set(path, data)  # 如果已经存在，则更新数据
            else:
                self.zk.create(path, data, ephemeral=True, makepath=True)  # 创建新的临时节点

            self.logger.info(f"Registered {role_str} node: {path}")
            if register_req.role == discovery_pb2.ROLE_PUBLISHER:
                for topic in register_req.topiclist:
                    self.hm.setdefault(topic, []).append(
                        (register_req.info.id, register_req.info.addr, register_req.info.port)
                    )
                    self.pubset.add((register_req.info.id, register_req.info.addr, register_req.info.port))
                self.cur_pubs += 1
                self.logger.info(f"Registered publisher: {register_req.info.id}, Topics: {register_req.topiclist}")

            elif register_req.role == discovery_pb2.ROLE_SUBSCRIBER:
                for topic in register_req.topiclist:
                    self.hm2.setdefault(topic, []).append(
                        (register_req.info.id, register_req.info.addr, register_req.info.port)
                    )
                self.cur_subs += 1
                self.logger.info(f"Registered subscriber: {register_req.info.id}, Topics: {register_req.topiclist}")

            elif register_req.role == discovery_pb2.ROLE_BOTH:
                self.broker_addr = register_req.info.addr
                self.broker_port = register_req.info.port
                self.logger.info(f"Registered broker: {register_req.info.id}, Addr: {self.broker_addr}, Port: {self.broker_port}")

            ready_resp = discovery_pb2.RegisterResp()
            ready_resp.status = discovery_pb2.STATUS_SUCCESS  # 确保返回 SUCCESS 状态
            discovery_resp = discovery_pb2.DiscoveryResp()
            discovery_resp.msg_type = discovery_pb2.MsgTypes.TYPE_REGISTER
            discovery_resp.register_resp.CopyFrom(ready_resp)
            self.mw_obj.handle_response(discovery_resp)

        except Exception as e:
            self.logger.error(f"Exception in register_request: {e}", exc_info=True)
            raise


    def lookup_response(self, lookup_req):
        self.logger.info("DiscoveryAppln::lookup_response started")
        self.logger.debug(f"lookup_req received: {lookup_req.topiclist}")
        self.logger.debug(f"Current hm state: {self.hm}")

        try:
            lookup_resp = discovery_pb2.LookupPubByTopicResp()
            if self.dissemination == "Broker":
                temp = discovery_pb2.RegistrantInfo()
                temp.id = "Broker"
                temp.addr = self.broker_addr
                temp.port = self.broker_port
                lookup_resp.array.append(temp)
            else:
                for topic in lookup_req.topiclist:
                    self.logger.debug(f"Processing topic: {topic}")
                    if topic in self.hm:
                        for tup in self.hm[topic]:
                            temp = discovery_pb2.RegistrantInfo()
                            temp.id = tup[0]
                            temp.addr = tup[1]
                            temp.port = tup[2]
                            lookup_resp.array.append(temp)
                    else:
                        self.logger.warning(f"No publisher found for topic {topic}")

            self.logger.debug(f"lookup_resp after processing: {lookup_resp}")

            discovery_resp = discovery_pb2.DiscoveryResp()
            discovery_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            discovery_resp.lookup_resp.CopyFrom(lookup_resp)

            self.logger.debug(f"DiscoveryAppln::lookup_response - Sending response: {discovery_resp}")
            self.mw_obj.handle_response(discovery_resp)

        except Exception as e:
            self.logger.error(f"Exception in lookup_response: {e}", exc_info=True)
            raise

    def pubslookup_response(self, lookup_req):
        self.logger.info("DiscoveryAppln::pubslookup_response started")
        try:
            lookup_resp = discovery_pb2.LookupPubByTopicResp()
            for topic, pub_list in self.hm.items():
                for tup in pub_list:
                    temp = discovery_pb2.RegistrantInfo()
                    temp.id = tup[0]
                    temp.addr = tup[1]
                    temp.port = tup[2]
                    lookup_resp.array.append(temp)
            discovery_resp = discovery_pb2.DiscoveryResp()
            discovery_resp.msg_type = discovery_pb2.MsgTypes.TYPE_LOOKUP_ALL_PUBS
            discovery_resp.lookup_resp.CopyFrom(lookup_resp)
            self.mw_obj.handle_response(discovery_resp)
        except Exception as e:
            self.logger.error(f"Exception in pubslookup_response: {e}")
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Discovery Application")
    # parser.add_argument("-P", "--pubs", type=int, default=1, help="total number of publishers in the system")
    # parser.add_argument("-S", "--subs", type=int, default=1, help="total number of subscribers in the system")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level")
    parser.add_argument("-p", "--port", type=int, default=5555, help="Port number on which the Discovery service runs")
    parser.add_argument("--group", default="0", help="Load balancing group ID for Discovery")
    return parser.parse_args()


def main():
    try:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger = logging.getLogger("DiscoveryAppln")

        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)

        disc_app = DiscoveryAppln(logger)
        disc_app.configure(args)
        disc_app.driver()

    except Exception as e:
        logger.error(f"Exception in main: {e}", exc_info=True)


if __name__ == "__main__":
    main()