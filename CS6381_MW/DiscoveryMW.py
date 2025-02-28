###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Modified: [Your Name]
# Function: Middleware for Discovery application
#
###############################################
import zmq
from CS6381_MW import discovery_pb2


class DiscoveryMW():

    def __init__(self, logger):
        self.logger = logger
        self.rep = None
        self.poller = None
        self.addr = None
        self.port = None
        self.upcall_obj = None
        self.handle_events = True

    def configure(self, args):
        ''' Initialize the object '''
        try:
            self.logger.info("DiscoveryMW::configure")

            self.port = args.port
            self.addr = "127.0.0.1"

            context = zmq.Context()
            self.poller = zmq.Poller()

            self.rep = context.socket(zmq.REP)
            bind_string = f"tcp://*:{self.port}"
            self.rep.bind(bind_string)
            self.logger.info(f"DiscoveryMW binding to tcp://*:{self.port}")

            self.poller.register(self.rep, zmq.POLLIN)
            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e:
            self.logger.error(f"Exception in configure: {e}", exc_info=True)
            raise

    def event_loop(self, timeout=None):
        try:
            self.logger.info("DiscoveryMW::event_loop - run the event loop")

            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                if self.rep in events:
                    timeout = self.handle_request()

            self.logger.info("DiscoveryMW::event_loop - out of the event loop")

        except Exception as e:
            self.logger.error(f"Exception in event_loop: {e}", exc_info=True)
            raise

    def handle_request(self):
        try:
            self.logger.debug("DiscoveryMW::handle_request")

            bytes_rcvd = self.rep.recv()
            self.logger.debug(f"DiscoveryMW::handle_request - Received {len(bytes_rcvd)} bytes")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(bytes_rcvd)

            # Redirection if not leader
            if not self.upcall_obj.is_leader:
                resp = discovery_pb2.DiscoveryResp()
                resp.msg_type = discovery_pb2.TYPE_FAILURE
                primary_data, _ = self.upcall_obj.zk.get(self.upcall_obj.leader_path)
                resp.redirect_addr = primary_data.decode()
                self.rep.send(resp.SerializeToString())
                return

            # Handle the request
            if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_request(disc_req.register_req)
                # register_resp = discovery_pb2.RegisterResp()
                # register_resp.status = discovery_pb2.STATUS_SUCCESS
                # discovery_resp = discovery_pb2.DiscoveryResp()
                # discovery_resp.msg_type = discovery_pb2.TYPE_REGISTER
                # discovery_resp.register_resp.CopyFrom(register_resp)
                # self.rep.send(discovery_resp.SerializeToString())

            # elif disc_req.msg_type == discovery_pb2.TYPE_ISREADY:
            #     timeout = self.upcall_obj.isready_response(disc_req.isready_req)

            else:
                self.logger.error("Unrecognized message type received")
                raise Exception("Unrecognized response message")

            return timeout

        except Exception as e:
            self.logger.error(f"Exception in handle_request: {e}", exc_info=True)
            raise

    def handle_response(self, resp):
        try:
            buf_to_send = resp.SerializeToString()
            self.rep.send(buf_to_send)
        except Exception as e:
            self.logger.error(f"Exception in handle_response: {e}", exc_info=True)
            raise

    def set_upcall_handle(self, upcall_obj):
        ''' Set upcall handle '''
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        ''' Disable event loop '''
        self.handle_events = False
