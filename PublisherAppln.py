###############################################
#
# Author: Modified by [Your Name]
# Function: Publisher application with ZooKeeper registration
#
###############################################

import os
import sys
import time
import argparse
import configparser
import logging
import json
from enum import Enum
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, ConnectionClosedError
from topic_selector import TopicSelector
from CS6381_MW.PublisherMW import PublisherMW
from CS6381_MW import discovery_pb2


class PublisherAppln:
    """Publisher application with ZooKeeper-based discovery registration"""

    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        DISSEMINATE = 3
        COMPLETED = 4

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.iters = None
        self.frequency = None
        self.num_topics = None
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None
        self.logger = logger
        self.zk_hosts = None
        self.zk = None  # ZooKeeper client

    def configure(self, args):
        """Initialize the Publisher application"""
        try:
            self.logger.info("PublisherAppln::configure")
            self.state = self.State.CONFIGURE

            self.name = args.name
            self.iters = args.iters
            self.frequency = args.frequency
            self.num_topics = args.num_topics

            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            self.zk_hosts = config["ZooKeeper"]["Hosts"]

            # 初始化 ZooKeeper 连接
            self.zk = KazooClient(hosts=self.zk_hosts)
            self.zk.start(timeout=15)
            self.logger.info(f"Connected to ZooKeeper at {self.zk_hosts}")
            self.mw_obj = PublisherMW(self.logger)
            self.mw_obj.configure(args, self.zk)
            # 确保 Publisher 在 ZooKeeper 中注册
            self.register_with_zookeeper()

            # 选择发布的主题
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)
            self.logger.info(f"PublisherAppln::configure - selected topics: {self.topiclist}")

            # 初始化中间件
            self.logger.info("PublisherAppln::configure - initializing middleware")


            self.logger.info("PublisherAppln::configure - configuration complete")

        except ConnectionClosedError as e:
            self.logger.error(f"ZooKeeper connection failed: {e}")
            raise
        except Exception as e:
            raise e

    def register_with_zookeeper(self):
      """将 Publisher 注册到 ZooKeeper 的正确路径"""
      try:
        path = f"/registrations/publishers/{self.name}"  # 修改注册路径
        data = json.dumps({
          "addr": self.mw_obj.addr,
          "port": self.mw_obj.port,
          "topics": self.topiclist
        }).encode()

        if self.zk.exists(path):
          self.logger.info(f"Publisher node already exists: {path}, updating data")
          self.zk.set(path, data)  # 如果已经存在，则更新数据
        else:
          self.logger.info(f"Creating Publisher node in ZooKeeper: {path}")
          self.zk.create(path, value=data, ephemeral=True, makepath=True)  # 创建新的临时节点

      except Exception as e:
        self.logger.error(f"Failed to register Publisher in ZooKeeper: {e}")
        raise

    def driver(self):
        """Publisher 的主循环，等待 Discovery 选举完成后再启动"""
        try:
            self.logger.info("PublisherAppln::driver")

            self.mw_obj.setup_discovery_watch()

            # 等待 Discovery 选举完成
            while self.mw_obj.primary_discovery is None:
                self.logger.info("Waiting for Discovery leader election...")
                time.sleep(2)

            self.logger.info(f"Connected to Discovery leader: {self.mw_obj.primary_discovery}")

            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)

            self.logger.info("PublisherAppln::driver completed")

        except Exception as e:
            self.logger.error(f"PublisherAppln::Driver execution failed: {e}")
            raise

    def check_discovery_ready(self):
        """检查 Discovery 是否准备好接受请求"""
        try:
            isready_req = discovery_pb2.IsReadyReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)

            self.mw_obj.req.send(disc_req.SerializeToString())
            response = self.mw_obj.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(response)

            return disc_resp.isready_resp.status == discovery_pb2.STATUS_SUCCESS

        except Exception as e:
            self.logger.warning(f"Discovery readiness check failed: {e}")
            return False

    def invoke_operation(self):
        """State machine logic for Publisher operations"""
        try:
            self.logger.info("PublisherAppln::invoke_operation")

            if self.state == self.State.REGISTER:
                self.logger.info("PublisherAppln::invoke_operation - Registering with Discovery")
                self.mw_obj.register(self.name, self.topiclist)
                return None  # 继续等待 Discovery 响应

            elif self.state == self.State.DISSEMINATE:
                self.logger.info("PublisherAppln::invoke_operation - Starting dissemination")
                time.sleep(3)  # 等待 Subscriber 连接

                ts = TopicSelector()
                for _ in range(self.iters):
                    for topic in self.topiclist:
                        dissemination_data = ts.gen_publication(topic)
                        self.mw_obj.disseminate(self.name, topic, dissemination_data)

                    time.sleep(1 / float(self.frequency))  # 控制发送频率

                self.logger.info("PublisherAppln::invoke_operation - Dissemination completed")
                self.state = self.State.COMPLETED
                return 0  # 让事件循环退出

            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                self.zk.stop()
                self.zk.close()
                return None

            else:
                raise ValueError("Undefined state in PublisherAppln")

        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        """Handle registration response from Discovery"""
        try:
            self.logger.info("PublisherAppln::register_response")
            if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.info("PublisherAppln::register_response - Registration successful")
                self.state = self.State.DISSEMINATE
                return 0  # 让事件循环立即进入发布阶段
            else:
                self.logger.warning(f"PublisherAppln::register_response - Registration failed: {reg_resp.reason}")
                raise ValueError("Publisher must have a unique ID")
        except Exception as e:
            raise e

    def dump(self):
        """Log Publisher details"""
        try:
            self.logger.info("**********************************")
            self.logger.info("PublisherAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info(f"     Name: {self.name}")
            self.logger.info(f"     Lookup: {self.lookup}")
            self.logger.info(f"     Dissemination: {self.dissemination}")
            self.logger.info(f"     Num Topics: {self.num_topics}")
            self.logger.info(f"     TopicList: {self.topiclist}")
            self.logger.info(f"     Iterations: {self.iters}")
            self.logger.info(f"     Frequency: {self.frequency}")
            self.logger.info(f"     ZooKeeper: {self.zk_hosts}")
            self.logger.info("**********************************")
        except Exception as e:
            raise e


def parseCmdLineArgs():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Publisher Application")

    parser.add_argument("-n", "--name", default="pub", help="Unique publisher name")
    parser.add_argument("-a", "--addr", default="localhost", help="Publisher IP address")
    parser.add_argument("-p", "--port", type=int, default=5577, help="Publisher port")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=1, help="Number of topics to publish")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("-f", "--frequency", type=int, default=1, help="Publishing frequency")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="Number of iterations")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level")

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("PublisherAppln")
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    pub_app = PublisherAppln(logger)
    pub_app.configure(args)
    pub_app.driver()


if __name__ == "__main__":
    main()
