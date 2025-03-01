###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.
import os
import sys
import time
import argparse
import configparser
import logging
from enum import Enum
import zmq

from topic_selector import TopicSelector
from CS6381_MW.SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2

import json
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, ConnectionClosedError

class SubscriberAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        # ISREADY = 3
        LOOKUP = 4
        ACCEPT = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.num_topics = None
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None
        self.logger = logger

        self.zk = None                # 保存 ZooKeeper 客户端
        self.zk_hosts = None          # ZooKeeper 服务器地址

    def configure(self, args):
        """Initialize the middleware object."""
        try:
            self.logger.debug("SubscriberAppln::configure")
            self.name = args.name
            self.num_topics = args.num_topics

            # Parse configuration file
            self.logger.debug("SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # 读取 ZooKeeper 地址，并创建客户端
            self.zk_hosts = config["ZooKeeper"]["Hosts"]
            self.zk = KazooClient(hosts=self.zk_hosts)
            self.zk.start(timeout=15)
            self.logger.info(f"Connected to ZooKeeper at {self.zk_hosts}")

            # Get topic list
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)
            self.logger.info(f"SubscriberAppln::configure - selected topics: {self.topiclist}")
            
            # Initialize middleware
            self.logger.debug("SubscriberAppln::configure - initializing middleware object")
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args, self.zk)

            self.logger.info("SubscriberAppln::configure - configuration complete")
        except Exception as e:
            self.logger.error(f"Error in configure: {e}")
            raise e

    def driver(self):
        """Driver program."""
        try:
            self.logger.info("SubscriberAppln::driver")
            self.dump()
            self.logger.debug("SubscriberAppln::driver - setting up upcall handle")
            self.mw_obj.set_upcall_handle(self)

             # 设置对 Discovery Leader 的监控（调用 mw_obj 中新添加的方法）
            self.mw_obj.setup_discovery_watch()
            while self.mw_obj.primary_discovery is None:
                self.logger.info("Waiting for Discovery leader election...")
                time.sleep(2)
            self.logger.info(f"Connected to Discovery leader: {self.mw_obj.primary_discovery}")

            self.state = self.State.REGISTER
            self.mw_obj.event_loop()
            self.logger.info("SubscriberAppln::driver completed")
        except Exception as e:
            self.logger.error(f"Error in driver: {e}")
            raise e

    def invoke_operation(self):
        """Handle operations based on current state."""
        try:
            self.logger.info("SubscriberAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.debug("SubscriberAppln::invoke_operation - registering with discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                return None
            elif self.state == self.State.ISREADY:
                self.logger.debug("SubscriberAppln::invoke_operation - checking readiness")
                self.mw_obj.is_ready()
                return None
            elif self.state == self.State.LOOKUP:
                self.logger.info("SubscriberAppln::invoke_operation - performing lookup")
                self.mw_obj.plz_lookup(self.topiclist)
                return None
            elif self.state == self.State.ACCEPT:
                self.logger.info("SubscriberAppln::invoke_operation - ready to accept dissemination")
                return None
            else:
                raise ValueError("Undefined state in SubscriberAppln::invoke_operation")
        except Exception as e:
            self.logger.error(f"Error in invoke_operation: {e}")
            raise e

    def register_response(self, register_resp):
        """Handle register response."""
        try:
            self.logger.info("SubscriberAppln::register_response")
            if register_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.debug("SubscriberAppln::register_response - registration successful")
                self.state = self.State.LOOKUP
                return 0
            else:
                self.logger.error(f"Registration failed: {register_resp.reason}")
                raise ValueError("Registration failed")
        except Exception as e:
            self.logger.error(f"Error in register_response: {e}")
            raise e

    # def isready_response(self, isready_resp):
    #     """Handle isready response."""
    #     try:
    #         self.logger.info("SubscriberAppln::isready_response")
    #         if isready_resp.status == discovery_pb2.STATUS_SUCCESS:
    #             self.state = self.State.LOOKUP
    #         else:
    #             self.logger.debug("System not ready, retrying...")
    #             time.sleep(5)
    #         return 0
    #     except Exception as e:
    #         self.logger.error(f"Error in isready_response: {e}")
    #         raise e

    def lookup_response(self, lookup_resp):
        """Handle lookup response."""
        try:
            self.logger.info("SubscriberAppln::lookup_response")
            self.logger.debug(f"lookup_resp: {lookup_resp}")
            for entry in lookup_resp.array:
                self.mw_obj.lookup_bind(entry.addr, entry.port)
            self.state = self.State.ACCEPT
            return 0
        except Exception as e:
            self.logger.error(f"Error in lookup_response: {e}")
            raise e


    def dump(self):
        """Dump application state."""
        try:
            self.logger.info("**********************************")
            self.logger.info("SubscriberAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info(f"Name: {self.name}")
            self.logger.info(f"Lookup: {self.lookup}")
            self.logger.info(f"Num Topics: {self.num_topics}")
            self.logger.info(f"Topic List: {self.topiclist}")
            self.logger.info("**********************************")
        except Exception as e:
            self.logger.error(f"Error in dump: {e}")
            raise e


def parseCmdLineArgs():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Subscriber Application")
    parser.add_argument("-n", "--name", required=True, help="Unique name for subscriber")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address of subscriber (default: localhost)")
    parser.add_argument("-p", "--port", required=True, type=int, help="Port for subscriber communication, default=5577")
    # parser.add_argument("-d", "--discovery", required=True, help="Discovery service address (e.g., localhost:5555)")
    parser.add_argument("-T", "--num_topics", required=True, type=int, help="Number of topics to subscribe")
    parser.add_argument("-t", "--toggle", action="store_true", help="Enable or disable toggle for subscriber")
    parser.add_argument("-f", "--filename", default="latency.json", help="Filename for logging latency metrics")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level")
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("SubscriberAppln")
    try:
        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)
        sub_app = SubscriberAppln(logger)
        sub_app.configure(args)
        sub_app.driver()
    except Exception as e:
        logger.error(f"Exception in main: {e}")


if __name__ == "__main__":
    main()
