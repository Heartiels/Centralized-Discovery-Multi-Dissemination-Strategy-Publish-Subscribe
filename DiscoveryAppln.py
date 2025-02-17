###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.
import os
import sys
import time
import argparse
import configparser
import logging

from topic_selector import TopicSelector

from CS6381_MW.DiscoveryMW import DiscoveryMW

from CS6381_MW import discovery_pb2

from enum import Enum

class DiscoveryAppln():

    def __init__(self, logger):
        self.pubs = None
        self.subs = None
        self.mw_obj = None
        self.logger = logger
        self.lookup = None
        self.dissemination = None
        self.hm = {}
        self.hm2 = {}
        self.cur_pubs = 0
        self.cur_subs = 0
        self.pubset = set()

        self.broker_addr = None
        self.broker_port = None

    def configure(self, args):
        ''' Initialize the object '''

        try:
            self.logger.info("DiscoveryAppln::configure")

            # initialize our variables
            self.subs = args.subs
            self.pubs = args.pubs

            # Now, get the configuration object
            self.logger.debug("DiscoveryAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now setup up our underlying middleware object to which we delegate everything
            self.logger.debug("DiscoveryAppln::configure - initialize the middleware object")
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args)  # pass remainder of the args to the m/w object

            self.logger.info("DiscoveryAppln::configure - configuration complete")

        except Exception as e:
            raise e

    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("DiscoveryAppln::driver")

            # First ask our middleware to keep a handle to us to make upcalls.
            self.logger.debug("DiscoveryAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            # Start the event loop
            self.mw_obj.event_loop(timeout=0)  # start the event loop

            self.logger.info("DiscoveryAppln::driver completed")

        except Exception as e:
            raise e

    def register_request(self, register_req):
        self.logger.info("DiscoveryAppln::register_request started")

        try:
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

            else:
                self.logger.error("Invalid role in registration request")
                return

            # Send response
            ready_resp = discovery_pb2.RegisterResp()
            ready_resp.status = discovery_pb2.STATUS_SUCCESS
            discovery_resp = discovery_pb2.DiscoveryResp()
            discovery_resp.msg_type = discovery_pb2.MsgTypes.TYPE_REGISTER
            discovery_resp.register_resp.CopyFrom(ready_resp)
            self.mw_obj.handle_response(discovery_resp)

        except Exception as e:
            self.logger.error(f"Exception in register_request: {e}")
            raise

    def isready_response(self, isready_req):
        self.logger.info("DiscoveryAppln::isready_response started")

        try:
            ready_resp = discovery_pb2.IsReadyResp()
            if self.pubs + self.subs == self.cur_pubs + self.cur_subs:
                if self.dissemination == "Broker" and (not self.broker_addr or not self.broker_port):
                    ready_resp.status = discovery_pb2.STATUS_FAILURE
                    self.logger.info("DiscoveryAppln:: Broker not registered yet")
                else:
                    ready_resp.status = discovery_pb2.STATUS_SUCCESS
                    self.logger.info("DiscoveryAppln:: SUCCESS; DISSEMINATION CAN NOW BEGIN")
            else:
                ready_resp.status = discovery_pb2.STATUS_FAILURE

            discovery_resp = discovery_pb2.DiscoveryResp()
            discovery_resp.msg_type = discovery_pb2.MsgTypes.TYPE_ISREADY
            discovery_resp.isready_resp.CopyFrom(ready_resp)
            self.mw_obj.handle_response(discovery_resp)

        except Exception as e:
            self.logger.error(f"Exception in isready_response: {e}")
            raise

    def lookup_response(self, lookup_req):
        self.logger.info("DiscoveryAppln::lookup_response started")

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
                    if topic in self.hm:
                        for tup in self.hm[topic]:
                            temp = discovery_pb2.RegistrantInfo()
                            temp.id = tup[0]
                            temp.addr = tup[1]
                            temp.port = tup[2]
                            lookup_resp.array.append(temp)

            discovery_resp = discovery_pb2.DiscoveryResp()
            discovery_resp.msg_type = discovery_pb2.MsgTypes.TYPE_LOOKUP_PUB_BY_TOPIC
            discovery_resp.lookup_resp.CopyFrom(lookup_resp)
            self.mw_obj.handle_response(discovery_resp)

        except Exception as e:
            self.logger.error(f"Exception in lookup_response: {e}")
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

    parser.add_argument("-P", "--pubs", type=int, default=1, help="total number of publishers in the system")
    parser.add_argument("-S", "--subs", type=int, default=1, help="total number of subscribers in the system")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level")
    parser.add_argument("-p", "--port", type=int, default=5555, help="Port number on which the Discovery service runs (default: 5555)")

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
        logger.error(f"Exception in main: {e}")

if __name__ == "__main__":
    main()
