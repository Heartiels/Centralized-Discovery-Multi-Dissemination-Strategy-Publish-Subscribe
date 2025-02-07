###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2


class DiscoveryMW():

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.rep = None  # will be a ZMQ REP socket to talk to Discovery service
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop

    def configure(self, args):
        ''' Initialize the object '''

        try:
            self.logger.info("DiscoveryMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = "10.0.0.1"  # Updated to use only the IP address

            # Next get the ZMQ context
            self.logger.debug("DiscoveryMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("DiscoveryMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REP socket
            self.logger.debug("DiscoveryMW::configure - obtain REP socket")
            self.rep = context.socket(zmq.REP)

            bind_string = f"tcp://*:{self.port}"
            self.rep.bind(bind_string)

            # Register the REP socket for incoming events
            self.logger.debug("DiscoveryMW::configure - register the REP socket for incoming replies")
            self.poller.register(self.rep, zmq.POLLIN)

            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e:
            raise e

    def event_loop(self, timeout=None):

        try:
            self.logger.info("DiscoveryMW::event_loop - run the event loop")

            while self.handle_events:  # it starts with a True value
                # poll for events with the specified timeout
                events = dict(self.poller.poll(timeout=timeout))

                if self.rep in events:
                    timeout = self.handle_request()

            self.logger.info("DiscoveryMW::event_loop - out of the event loop")

        except Exception as e:
            raise e

    #################################################################
    # handle an incoming reply
    ##################################################################
    def handle_request(self):

        try:
            self.logger.debug("DiscoveryMW::handle_request")

            # Receive all the bytes
            bytes_rcvd = self.rep.recv()

            # Deserialize the bytes using protobuf
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(bytes_rcvd)

            # Handle the request based on its message type
            if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_request(disc_req.register_req)

            elif disc_req.msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isready_response(disc_req.isready_req)

            elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                timeout = self.upcall_obj.pubslookup_response(disc_req.lookup_req)

            elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.lookup_response(disc_req.lookup_req)

            else:
                self.logger.error("Unrecognized message type received")
                raise Exception("Unrecognized response message")

            return timeout

        except Exception as e:
            self.logger.error(f"Exception in handle_request: {e}")
            raise e

    #################################################################
    # handle an outgoing response
    ##################################################################
    def handle_response(self, resp):
        try:
            buf_to_send = resp.SerializeToString()
            self.logger.debug(f"Sending serialized response: {buf_to_send}")
            self.rep.send(buf_to_send)
        except Exception as e:
            self.logger.error(f"Exception in handle_response: {e}")
            raise e

    ########################################
    # set upcall handle
    ########################################
    def set_upcall_handle(self, upcall_obj):
        ''' Set upcall handle '''
        self.upcall_obj = upcall_obj

    ########################################
    # disable event loop
    ########################################
    def disable_event_loop(self):
        ''' Disable event loop '''
        self.handle_events = False
