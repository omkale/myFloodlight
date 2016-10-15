import logging
import zmq

class ZMQueueDevice(object):
    def __init__(self):
        logging.basicConfig(format='[%(levelname)s]: %(message)s',level=logging.INFO)
        return

    def start(self,name,clie):
        try:
            context = zmq.Context(1)
            # Client side
            clientside = context.socket(zmq.XREP)
            clientside.bind("tcp://0.0.0.0:%s"%str(clie))
            # Server side
            serverside = context.socket(zmq.XREQ)
            serverside.bind("tcp://0.0.0.0:%s"%str(name))

            logging.info("Starting Queue...")
            zmq.device(zmq.QUEUE, clientside, serverside)
        except Exception, e:
            logging.info("[QUEUE] Failed reason: %s",e)
            logging.info("[QUEUE] Going down...")
        finally:
            pass
            clientside.close()
            serverside.close()
            context.term()
        return

qdevice = ZMQueueDevice()
