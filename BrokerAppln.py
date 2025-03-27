###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 
import os
import sys
import time
import argparse
import configparser
import logging
import json
from enum import Enum

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2

class BrokerAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        ADDPUBS = 3
        DISSEMINATION = 4
        COMPLETED = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.name = None
        self.num_topics = 0
        self.topiclist = []
        self.lookup = None
        self.dissemination = None

        self.zk = None
        self.is_leader = False
        self.election_node = None
        self.group_id = "0"  # 默认组号
        self.election_path = f"/brokers/election/group_{self.group_id}"
        self.leader_path = f"/brokers/leader/group_{self.group_id}"

        self.mw_obj = None

    def configure(self, args):
        try:
            self.logger.info("BrokerAppln::configure")
            self.state = self.State.CONFIGURE
            self.name = args.name
            self.num_topics = args.num_topics

            self.logger.debug("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            self.group_id = args.group if hasattr(args, "group") else "0"
            self.election_path = f"/brokers/election/group_{self.group_id}"
            self.leader_path = f"/brokers/leader/group_{self.group_id}"

            zk_hosts = config["ZooKeeper"]["Hosts"]
            self.zk = KazooClient(hosts=zk_hosts)
            self.zk.start()
            self.logger.info(f"Connected to ZooKeeper at {zk_hosts}")

            self.zk.ensure_path(self.election_path)

            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args, self.zk)
            self.mw_obj.set_upcall_handle(self)

            self.start_election()

            self.logger.info("BrokerAppln::configure - configuration complete")
        except Exception as e:
            self.logger.error(f"BrokerAppln::configure error: {e}", exc_info=True)
            raise e

    def start_election(self):
        try:
            node_value = f"{self.mw_obj.addr}:{self.mw_obj.port}".encode("utf-8")
            self.election_node = self.zk.create(
                self.election_path + "/node_",
                value=node_value,
                ephemeral=True,
                sequence=True
            )
            self.leaf_node = self.election_node.split("/")[-1]
            self.logger.info(f"[{self.name}] Created election node {self.election_node}")
            self.check_election_status()
        except Exception as e:
            self.logger.error(f"start_election error: {e}", exc_info=True)
            raise e
    
    def check_election_status(self):
        try:
            children = self.zk.get_children(self.election_path)
            children.sort()
            my_index = children.index(self.leaf_node)
            if my_index == 0:
                self.become_leader()
            else:
                predecessor = children[my_index - 1]
                pred_path = f"{self.election_path}/{predecessor}"
                self.logger.info(f"[{self.name}] I am backup. Watching predecessor: {pred_path}")

                @self.zk.DataWatch(pred_path)
                def watch_pred(data, stat):
                    if data is None:
                        self.logger.info(f"[{self.name}] Predecessor gone, re-checking election status")
                        self.check_election_status()
        except Exception as e:
            self.logger.error(f"check_election_status error: {e}", exc_info=True)
            raise e
    
    def become_leader(self):
        try:
            self.is_leader = True
            self.state = self.State.REGISTER
            node_value = f"{self.mw_obj.addr}:{self.mw_obj.port}".encode("utf-8")
            if self.zk.exists(self.leader_path):
                self.zk.delete(self.leader_path)
            self.zk.create(self.leader_path, node_value, ephemeral=True, makepath=True)
            self.logger.info(f"[{self.name}] ***** Elected as PRIMARY Broker *****")
            self.mw_obj.enable_pubsub()

        except Exception as e:
            self.logger.error(f"become_leader error: {e}", exc_info=True)
            raise e

    def become_backup(self):
        self.is_leader = False
        self.logger.info(f"[{self.name}] I am now BACKUP. Disabling pub-sub.")
        self.mw_obj.disable_pubsub()


    def driver(self):
        try:
            self.logger.info("BrokerAppln::driver")
            self.dump()

            self.logger.debug("BrokerAppln::driver - setting up upcall handle")
            self.mw_obj.event_loop(timeout=1000)

            self.logger.info("BrokerAppln::driver completed")
        except Exception as e:
            self.logger.error(f"BrokerAppln::driver error: {e}", exc_info=True)
            raise e

    def register_response(self, reg_resp):
        self.logger.info("BrokerAppln::register_response")
        if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.debug("BrokerAppln::register_response - registration successful")
            self.state = self.State.ADDPUBS
        else:
            self.logger.error(f"Registration failed: {reg_resp.reason}")
            raise ValueError("Broker registration failed")
        return 0

    def isready_response(self, isready_resp):
        self.logger.info("BrokerAppln::isready_response")
        if isready_resp.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.debug("Discovery is ready => proceed to ADDPUBS")
            self.state = self.State.ADDPUBS
        else:
            self.logger.debug("Discovery not ready, retrying in 5s")
            time.sleep(5)
        return 0

    def pubslookup_response(self, lookup_resp):
        self.logger.info("BrokerAppln::pubslookup_response")
        for pub in lookup_resp.array:
            if pub.id == self.name:
                continue
            self.mw_obj.lookup_bind(pub.addr, pub.port)
            self.logger.debug(f"Bound to publisher {pub.id} at {pub.addr}:{pub.port}")
        self.state = self.State.DISSEMINATION
        return 0

    def invoke_operation(self):
        try:
            self.logger.debug(f"BrokerAppln::invoke_operation - current state: {self.state}")
            if not self.is_leader:
                return 1000

            if self.state == self.State.REGISTER:
                self.mw_obj.register(self.name, [])
                return None
            elif self.state == self.State.ADDPUBS:
                self.mw_obj.request_pubs()
                return None
            elif self.state == self.State.DISSEMINATION:
                return 1000
            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                return None
            else:
                return 1000

        except Exception as e:
            self.logger.error(f"invoke_operation error: {e}", exc_info=True)
            raise e

    def dump(self):
        try:
            self.logger.info("**********************************")
            self.logger.info("BrokerAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info(f"     Name: {self.name}")
            self.logger.info(f"     Lookup: {self.lookup}")
            self.logger.info(f"     Dissemination: {self.dissemination}")
            self.logger.info(f"     Num Topics: {self.num_topics}")
            self.logger.info(f"     Topic List: {self.topiclist}")
            self.logger.info("**********************************")
        except Exception as e:
            self.logger.error(f"dump error: {e}", exc_info=True)
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-n", "--name", default="broker", help="Unique name for the broker")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker IP address")
    parser.add_argument("-p", "--port", type=int, default=5570, help="Broker port")
    # parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service address")
    parser.add_argument("-T", "--num_topics", type=int, default=5, help="Number of topics to handle")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("--group", default="0", help="Load balancing group ID for Broker")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level")
    return parser.parse_args()

def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("BrokerAppln")
    try:
        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)
        broker_app = BrokerAppln(logger)
        broker_app.configure(args)
        broker_app.driver()
    except Exception as e:
        logger.error(f"Exception in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
