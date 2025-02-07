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
from enum import Enum
from topic_selector import TopicSelector
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2

class BrokerAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        ISREADY = 3
        ADDPUBS = 4
        DISSEMINATION = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.name = None
        self.num_topics = None
        self.topiclist = None
        self.lookup = None
        self.dissemination = None
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

            self.logger.debug("BrokerAppln::configure - selecting topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)

            self.logger.debug("BrokerAppln::configure - initializing middleware")
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args)

            self.logger.info("BrokerAppln::configure - configuration complete")
        except Exception as e:
            self.logger.error(f"Error during configuration: {e}")
            raise e

    def driver(self):
        try:
            self.logger.info("BrokerAppln::driver")
            self.dump()

            self.logger.debug("BrokerAppln::driver - setting up upcall handle")
            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)
            self.logger.info("BrokerAppln::driver completed")
        except Exception as e:
            self.logger.error(f"Error in driver: {e}")
            raise e

    def register_response(self, reg_resp):
        try:
            self.logger.info("BrokerAppln::register_response")
            if reg_resp.status != discovery_pb2.STATUS_FAILURE:
                self.logger.debug("BrokerAppln::register_response - registration successful")
                self.state = self.State.ISREADY
                return 0
            else:
                self.logger.error(f"Registration failed: {reg_resp.reason}")
                raise ValueError("Broker registration failed")
        except Exception as e:
            self.logger.error(f"Error in register_response: {e}")
            raise e

    def isready_response(self, isready_resp):
        try:
            self.logger.info("BrokerAppln::isready_response")
            if isready_resp.status == discovery_pb2.STATUS_FAILURE:
                self.logger.debug("Discovery service not ready, retrying...")
                time.sleep(5)
            else:
                self.logger.debug("Discovery service is ready")
                self.state = self.State.ADDPUBS
            return 0
        except Exception as e:
            self.logger.error(f"Error in isready_response: {e}")
            raise e

    def pubslookup_response(self, lookup_resp):
        try:
            self.logger.info("BrokerAppln::pubslookup_response")
            # lookup_resp.array contains the list of publishers to which broker must connect.
            for pub in lookup_resp.array:
                addr = pub.addr
                port = pub.port
                self.mw_obj.lookup_bind(addr, port)
                self.logger.debug(f"BrokerAppln::pubslookup_response - connected to publisher {pub.id} at {addr}:{port}")
            self.state = self.State.DISSEMINATION
            self.logger.debug("BrokerAppln::pubslookup_response - publishers bound successfully")
            return 0
        except Exception as e:
            self.logger.error(f"Error in pubslookup_response: {e}")
            raise e

    def invoke_operation(self):
        try:
            self.logger.info("BrokerAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.debug("BrokerAppln::invoke_operation - registering with discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                return None
            elif self.state == self.State.ISREADY:
                self.logger.debug("BrokerAppln::invoke_operation - checking readiness with discovery service")
                self.mw_obj.is_ready()
                return None
            elif self.state == self.State.ADDPUBS:
                self.logger.debug("BrokerAppln::invoke_operation - requesting publishers info from discovery service")
                self.mw_obj.request_pubs()
                return None
            elif self.state == self.State.DISSEMINATION:
                # In dissemination state, the broker's event loop simply forwards messages.
                self.logger.debug("BrokerAppln::invoke_operation - dissemination in progress")
                return None
            else:
                raise ValueError("Unknown state in BrokerAppln::invoke_operation")
        except Exception as e:
            self.logger.error(f"Error in invoke_operation: {e}")
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
            self.logger.error(f"Error in dump: {e}")
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-n", "--name", default="broker", help="Unique name for the broker")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker IP address")
    parser.add_argument("-p", "--port", type=int, default=5570, help="Broker port")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service address")
    parser.add_argument("-T", "--num_topics", type=int, default=5, help="Number of topics to handle")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
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
        logger.error(f"Exception in main: {e}")

if __name__ == "__main__":
    main()
