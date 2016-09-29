import time
import sys
import threading
import logging
import signal
import os
from asyncElectionQ import al
start = 0
class HAController(object):
    def __init__(self,controllerID):
        self.controllerID = controllerID
        logging.basicConfig(format='[%(levelname)s]: %(message)s',level=logging.INFO)
        logging.info("[HACONTROLLER] Started controller %s"%self.controllerID)
        self.leader = None
        return

    def getLeader(self):
        return al.leader

    def xrun(self):
        try:
            while self.leader is None:
                self.leader = self.getLeader()
                if(self.leader):
                    end = time.time()
                    logging.info("[HACONTROLLER] Got leader: %s"%self.leader)
                    logging.info("[HACONTROLLER MEASURE] Leader obatined (s): %.2f"%(end-start))
                    break
        except Exception,e:
            logging.info("[HACONTROLLER] Couldn't get the leader")
            print e
        logging.info("[HACONTROLLER] +++++++++++++++++ Calling Hooks:")
        return

    def start(self):
        t1 = threading.Thread(target=al.start).start()
        t2 = threading.Thread(target=self.xrun).start()
        return

if __name__ == '__main__':
    try:
        nem = sys.argv[1]
        clin = sys.argv[2]
        ID  = sys.argv[3]
        al.setName(str(nem))
        al.setControllerID(str(ID))
        al.setClientPort(str(clin))
        hac = HAController(str(ID))
        start = time.time()
        hac.start()
        signal.pause()
    except (KeyboardInterrupt, SystemExit):
        print "EXIT!"
        os._exit(0)
