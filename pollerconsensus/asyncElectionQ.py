import logging
import sys
import threading
import time
import math
import zmq
import os
import signal
import QueueDevice
from QueueDevice import qdevice

class States(object):
    def __init__(self):
        self.connect    = "CONNECT"
        self.elect      = "ELECT"
        self.coordinate = "COORDINATE"
        self.spin       = "SPIN"
        self.current = self.connect

class AsyncElection(object):
    def __init__(self):
        self.name = None
        self.clientPort = None
        self.state = States()
        self.contextc = zmq.Context()

        # Keeping track of other servers
        self.serverList = []
        self.allServerList =[]

        # Set of servers to establish connections with
        self.connectSet = set()

        # Map of servers which are already connected,
        # with their connection objects
        self.connectionDict = dict()

        # Our own controller ID
        self.controllerID = None

        # Receiving values from other servers during election
        self.rcvdVal = set()
        self.IWon = None

        # Indicates who the current leader of the system is
        self.leader = None
        self.tempLeader = None

        # Standardized time factor for sleeps, retry time,
        # number of pulses to try before expiring.
        self.retryConnectionLatency = 0
        self.socketTimeOut = 500
        self.numberOfPulses = 1
        self.chill = 5

        return

    def setName(self,name):
        self.name = str(name)
        logging.info("Server Port to init: %s"%self.name)
        return

    def setControllerID(self,cid):
        self.controllerID = str(cid)
        logging.info("Controller ID to init: %s"%self.controllerID)
        return

    def setClientPort(self,port):
        self.clientPort = str(port)
        logging.info("Client Port to init: %s"%self.clientPort)
        return

    def preStart(self,configFile="server2.config"):
        conf = open(configFile,'r')
        for line in conf.readlines():
            line = line.rstrip()
            self.serverList.append(line)
            self.allServerList.append(line)
        self.serverList.remove(self.clientPort)

        self.connectSet = set(self.serverList)
        self.totalRounds = len(self.serverList)
        if(self.totalRounds >= 2):
            self.majority = int(round(0.51*self.totalRounds))
        else:
            self.majority = 1

        message = ' : '.join(str(e) for e in self.serverList)
        logging.info("Other Servers: %s Majority: %s" % (message,self.majority))

        return


    def sendHeartBeat(self):
        # The leader will send a heartbeat message, and will expect a
        # reply from a majority of acceptors.
        noSet = set()
        for client, client_sock in self.connectionDict.iteritems():
            try:
                client_sock.send("HEARTBEAT "+self.controllerID)
                reply = client_sock.recv()
                if(reply == "NO"):
                    noSet.add(client)
                logging.info("[CLIENT %s] Received HEARTBEAT ACK from server %s"%(client,reply))
            except Exception, e:
                logging.info("[CLIENT %s] Couldn't receive HEARTBEAT ACK from server for election"%client)

        if(len(noSet) > 0):
            self.leader = None

        return

    def sendIWon(self):
        # The winner of the election, or the node that hasn't received an "OK"
        # message during the course of the election, gets to send an "IWON" message
        # to all the other nodes, essentially asserting its dominance at the end of
        # the election. It would still need to receive a "LEADOK" message from a
        # majority of the nodes to actually become the leader.
        for client, client_sock in self.connectionDict.iteritems():
            try:
                client_sock.send("IWON "+self.controllerID)
                reply = client_sock.recv()
                logging.info("[CLIENT %s] Received ACK from server %s"%(client,reply))
            except Exception, e:
                logging.info("[CLIENT %s] Couldn't receive ACK from server for election"%client)

        return

    def sendLeaderMsg(self):
        # Send a "LEADER" message and try to receive "LEADOK" messages from all the
        # clients. If count("LEADOK") > majority, then you have won the election and
        # hence become the leader.
        acceptors = set()
        for client, client_sock in self.connectionDict.iteritems():
            try:
                client_sock.send("LEADER "+self.controllerID)
                reply = client_sock.recv()
                logging.info("[CLIENT %s] Received LEADOK from server %s"%(client,reply))
                if("LEADOK" in reply):
                    leadok,port = reply.split()
                    logging.info("Acceptor: %s"%port)
                    acceptors.add(port)
            except Exception, e:
                logging.info("[CLIENT %s] Couldn't receive ACK from server for election"%client)

        if (len(acceptors) >= self.majority):
            self.leader = self.controllerID
            self.state.current = self.state.coordinate
        else:
            self.leader = None
            self.state.current = self.state.elect

        return

    def setAsLeader(self):
        # The leader will set itself as leader during each co-ordinate phase,
        # to ensure that all nodes see it as the leader.
        noSet = set()
        for client, client_sock in self.connectionDict.iteritems():
            try:
                client_sock.send("SETLEAD "+self.controllerID)
                reply = client_sock.recv()
                if(reply == "NO"):
                    noSet.add(client)
                logging.info("[CLIENT %s] Received SETLEAD ACK from server %s"%(client,reply))
            except Exception, e:
                logging.info("[CLIENT %s] Couldn't receive SETLEAD ACK from server for election"%client)

        if(len(noSet) >= self.majority):
            self.leader = None

        return

    def checkForLeader(self):
        # Ask all the nodes, if they are the leader. You should get an answer from
        # only one node, claiming it is the leader.
        leaderSet = set()
        for client, client_sock in self.connectionDict.iteritems():
            try:
                client_sock.send("YOU?")
                reply = client_sock.recv()
                if not(reply == "NO"):
                    leaderSet.add(reply)
                elif(reply == "NO"):
                    logging.info("[CLIENT %s] Check Leader: Received NO"%client)
                    continue
            except Exception, e:
                logging.info("[CLIENT %s] Couldn't receive YOULEADER? Answer from server for election"%client)

        logging.info("Leader Set %s"%str(leaderSet))
        leaderSet.discard('')
        if(None in leaderSet):
            # If the leader set contains None.
            logging.info("LeaderSet contains None")
            if(len(leaderSet) == 1):
                self.leader = None
                logging.info("Current leader is None")
            elif(len(leaderSet) ==2):
                leaderSet.discard(None)
                self.leader = leaderSet.pop()
                logging.info("Current leader is %s"%self.leader)
            elif(len(leaderSet)>2):
                logging.info("Split brain!")
                self.leader = None
                logging.info("Current Leader is None.")
            elif(len(leaderSet)<1):
                self.leader = None
                logging.info("Current leader is None.")
        else:
            # If the leader set doesn't contain None.
            if(len(leaderSet) == 1):
                self.leader = leaderSet.pop()
                logging.info("Current leader is %s"%self.leader)
            elif(len(leaderSet)>1):
                logging.info("Split Brain!")
                self.leader = None
                logging.info("Current Leader is None.")
            elif(len(leaderSet)<1):
                self.leader = None
                logging.info("Current leader is None.")


        return

    def processServerMsg(self,mssg):
        if("IWON" in mssg):
            iwon,port = mssg.split()
            self.tempLeader = port
            logging.info("[SERVER] Received IWON message from %s"%port)
            reply = "ACK"
        elif("LEADER" in mssg):
            l,port = mssg.split()
            if(self.tempLeader == port):
                reply = "LEADOK "+self.controllerID
            else:
                reply = "NO"
        elif("SETLEAD" in mssg):
            setl,port = mssg.split()
            if not(self.leader == self.controllerID):
                if(self.tempLeader == port):
                    self.leader = port
                    logging.info("[SERVER] Received SET LEAD  message from %s"%port)
                    if(self.leader):
                        reply = "ACK"
                    else:
                        reply = "NO"
                else:
                    reply = "NO"
            else:
                reply = "NO"
        elif("YOU?" in mssg):
            if(self.leader == self.controllerID):
                reply = self.leader
            else:
                reply = "NO"
        elif("HEARTBEAT" in mssg):
            h,leader = mssg.split()
            if(self.leader == leader):
                reply = "ACK"
            else:
                reply = "NO"
        elif("PULSE" in mssg):
            reply = "ACK"
        else:
            reply = "DONTCARE"
        return reply

    def giveLeader(self):
        return self.leader

    def electionLogic(self):
        nodes = list(self.allServerList)
        nodes.sort()
        nodes.reverse()

        for i in range(len(nodes)):
            if(self.connectionDict.has_key(nodes[i])):
                maxNode = nodes[i]
                break

        if(self.clientPort >= maxNode):
            maxNode = self.clientPort

        logging.info("[INFO] Current Active nodes: %s"%str(nodes))
        logging.info("[INFO] Max Node: %s"%str(maxNode))

        try:
            if(int(self.controllerID) == (int(maxNode)-5251)):
                self.leader = str(int(maxNode) - 5251)
            else:
                for i in range(self.numberOfPulses):
                    self.connectionDict[maxNode].send("PULSE")
                    reply = self.connectionDict[maxNode].recv()
                if(reply == "ACK"):
                    self.leader = str(int(maxNode) - 5251)
        except Exception, e:
            logging.info("[ELECTION] Unable to contact maxNode %s"%str(e))

        return

    def elect(self):
        # Min Set Election Algorithm
        # Just pick the min(PID) which is currently active to
        # be the leader.

        # Ensure majority are still connected
        if(len(self.connectionDict) < self.majority):
            time.sleep(self.retryConnectionLatency)
            return

        # Clear all election variables.
        self.tempLeader = None
        self.leader = None

        # Check if actually in elect state, majority connected
        if not(self.state.current == self.state.elect):
            return

        # Node joins AFTER Election:
        # To detect if leader has already been set, send an
        # "YOULEADER?" message, to which you get either "NO"
        # from the followers, or the PID from the leader.

        self.checkForLeader()

        # If a leader has been set, exit election state, and
        # go to SPIN.
        if(self.leader):
            self.state.current = self.state.spin
            return
        # End of Node joins AFTER Election.


        # Actual Election Logic:
        self.electionLogic()

        if(self.leader == self.controllerID):
            logging.info("I WON THE ELECTION!")
            self.sendIWon()
            self.sendLeaderMsg()
            self.setAsLeader()
        elif(self.leader is None):
            self.state.current = self.state.elect
        else:
            self.state.current = self.state.spin

        #End of Actual Election Logic.

        return

    def cases(self):
        while True:
            logging.info("Current state: %s"%self.state.current)
            if(self.state.current == "CONNECT"):
                # block until at least majority of servers connect
                self.blockTillConnected()
                # Finished connect state now, moving on to elect
            elif(self.state.current == "ELECT"):
                # Check for new nodes.
                self.checkForNewConnections()
                if(len(self.connectionDict) < self.majority):
                    self.state.current = self.state.connect
                # Start the election, after majority of the nodes have connected.
                self.elect()
                # Once election is done and leader is confirmed, either
                # spin or coordinate.
            elif(self.state.current == "SPIN"):
                # This is the resting state after election
                self.checkForNewConnections()
                if(self.leader == None):
                    self.state.current = self.state.elect
                    return
                # If you don't receive a heartbeat, go to elect
                logging.info("[FOLLOWER] ++++++++++++++++++++++++++++++++ Leader set to: %s"%self.leader)
                # Logic for nodes to go into election state when
                # leader is not set, OR if multiple leaders are set.
                self.checkForLeader()
                time.sleep(self.chill)
            elif(self.state.current == "COORDINATE"):
                # This is the resting state after election
                self.checkForNewConnections()
                if(self.leader == None):
                    self.state.current = self.state.elect
                    return
                logging.info("[LEADER] +++++++++++++++++++++++++++++++++ Leader set to: %s"%self.leader)
                self.sendIWon()
                # Keep the leader in COORDINATE state.
                self.sendLeaderMsg()
                self.setAsLeader()
                # Keep sending heartbeat, and receive majority of acceptors
                # else go to elect state.
                self.sendHeartBeat()
                time.sleep(self.chill)

        return

    ################################### NETWORKING FUNCTIONS ###############################

    def expireOldConnections(self):
        # Check if a connection is still alive by sending
        # 5+ pings. If not, then delete that connection from the
        # connectionDict.
        logging.info("Expiring stale connections...")
        delmark = {}
        for client, client_sock in self.connectionDict.iteritems():
            try:
                for i in range(self.numberOfPulses):
                    client_sock.send("PULSE")
                    reply = client_sock.recv()
                if not (reply == "ACK"):
                    logging.info("[CLIENT %s] Closing stale connection to %s"%(client,client))
                    client_sock.close(linger=None)
                    delmark[client] = client_sock
            except Exception, e:
                logging.info("[CLIENT %s] Encountered an exception while trying to contact %s"%(client,client))
                if(client_sock):
                    client_sock.close(linger=None)
                    delmark[client] = client_sock

        # Pop out all the old connections from the connection map
        for oldC in iter(delmark):
            self.connectionDict.pop(oldC)

        logging.info("Expired old connections.")

        return

    def checkForNewConnections(self):

        self.expireOldConnections()

        # Take the set difference between the already connected nodes
        # and the nodes in the serverList, to get the nodes to watch for.
        # (self.connectSet - self.connectionDict{nodes})
        self.connectSet = set(self.serverList)

        diffSet = set()
        connectedNodes = set()

        for client, client_sock in self.connectionDict.iteritems():
            connectedNodes.add(client)

        diffSet = self.connectSet.difference(connectedNodes)
        logging.info("New connections to look for: %s"%str(diffSet))

        # Establish connections to the other nodes if available.
        for client in diffSet:
            try:
                client_sock = self.contextc.socket(zmq.REQ)
                client_sock.connect("tcp://127.0.0.1:"+str(client))
                # Setting send and receive timeouts
                client_sock.RCVTIMEO = self.socketTimeOut
                client_sock.SNDTIMEO = self.socketTimeOut
                client_sock.send("PULSE")
                reply = client_sock.recv()
                if(reply =="ACK"):
                    logging.info("[CLIENT %s]: %s"%(client,client_sock))
                    if not(self.connectionDict.has_key(client)):
                        self.connectionDict[client] = client_sock
                    else:
                        logging.info("[CLIENT %s] Closed existing socket %s"%(client,self.connectionDict[client]))
                        self.connectionDict[client].close()
                        self.connectionDict[client] = client_sock
                        logging.info("[CLIENT %s] Added a new socket %s"%(client,self.connectionDict[client]))
                else:
                    logging.info("[CLIENT %s] Received bad reply"%client)
                    rc = client_sock.close()
                    logging.info("[CLIENT %s] Closed socket %s"%(client,rc))
            except Exception, e:
                if(client_sock):
                    rc = client_sock.close(linger=None)
                logging.info("[CLIENT %s] Exception: %s"%(client,e))
                logging.info("[CLIENT %s] Could not connect to client %s, closed socket"%(client,client))

        return

    def blockTillConnected(self):
        # Reset all variables
        self.connectSet = set(self.serverList)

        # Close all previous connections
        delmark = {}
        for portno,sockObj in self.connectionDict.iteritems():
            try:
                logging.info("Closing connection %s %s"%(portno,sockObj))
                v.close(linger=None)
                delmark[portno] = sockObj
            except:
                delmark[portno] = sockObj
                logging.info("Failed to close connection: %s"%portno)

        # Pop out all the old connections from the connection map
        for oldC in iter(delmark):
            self.connectionDict.pop(oldC)


        self.connectionDict = dict()
        # Spin till you get majority
        while (len(self.connectionDict) < self.majority):
            self.connectClients()
            time.sleep(self.retryConnectionLatency)

        self.state.current = self.state.elect
        return

    def connectClients(self):
        logging.info("To Connect: %s"%str(self.connectSet))
        logging.info("Connected: %s"%str(self.connectionDict))

        diffSet = set()
        connectedNodes = set()

        for client, client_sock in self.connectionDict.iteritems():
            connectedNodes.add(client)

        diffSet = self.connectSet.difference(connectedNodes)
        logging.info("Diff Set of To connect - Connected: %s"%str(diffSet))

        # Try conecting to all clients in ToConnect Set and store successful ones in a Dict
        for client in diffSet:
            try:
                client_sock = self.contextc.socket(zmq.REQ)
                client_sock.connect("tcp://127.0.0.1:"+str(client))
                # Setting send and receive timeouts
                client_sock.RCVTIMEO = self.socketTimeOut
                client_sock.SNDTIMEO = self.socketTimeOut
                client_sock.send("PULSE")
                reply = client_sock.recv()
                if(reply =="ACK"):
                    logging.info("[CLIENT %s]: %s"%(client,client_sock))
                    if not(self.connectionDict.has_key(client)):
                        self.connectionDict[client] = client_sock
                    else:
                        logging.info("[CLIENT %s] Closed existing socket %s"%(client,self.connectionDict[client]))
                        self.connectionDict[client].close()
                        self.connectionDict[client] = client_sock
                        logging.info("[CLIENT %s] Added a new socket %s"%(client,self.connectionDict[client]))
                else:
                    logging.info("[CLIENT %s] Received bad reply"%client)
                    rc = client_sock.close()
                    logging.info("[CLIENT %s] Closed socket %s"%(client,rc))
            except Exception, e:
                if(client_sock):
                    rc = client_sock.close(linger=None)
                logging.info("[CLIENT %s] Exception: %s"%(client,e))
                logging.info("[CLIENT %s] Could not connect to client %s, closed socket"%(client,client))

        # Delete already connected clients from ToConnect Set
        for port, sock in self.connectionDict.iteritems():
            if(port in self.connectSet):
                self.connectSet.discard(port)
                logging.info("Discarding already connected port %s"%port)

        return

    def run(self):
        server_socket = self.contextc.socket(zmq.REP)
        server_socket.connect("tcp://127.0.0.1:%s"%self.name)
        # Setting send and receive timeouts
        server_socket.RCVTIMEO = self.socketTimeOut
        server_socket.SNDTIMEO = self.socketTimeOut
        pollSet = set()
        poller = zmq.Poller()
        for client_sock in self.connectionDict.itervalues():
            poller.register(client_sock, zmq.POLLIN)
            pollSet.add(client_sock)
        while True:
            newPollSet = pollSet.symmetric_difference(set(self.connectionDict.values()))
            for value in newPollSet:
                if value in pollSet:
                    poller.unregister(value)
                    pollSet.remove(value)
                else:
                    poller.register(value)
                    pollSet.add(value)
            try:
                socks = dict(poller.poll())
                for client_sock in pollSet:
                    if client_sock in socks and socks[client_sock] == zmq.POLLIN:
                        stg = server_socket.recv()
                        #logging.info("[SERVER] Server rcvd: %s"%stg)
                        reply = self.processServerMsg(stg)
                        server_socket.send(reply)
            except Exception, e:
                logging.info("[SERVER] %s"%e)
        return

    def client(self):
        logging.info("Connecting to clients...")
        self.cases()
        return

    def zmqueue(self):
        qdevice.start(self.name,self.clientPort)
        return

    def start(self):
        self.preStart()
        t0 = threading.Thread(target=self.zmqueue).start()

        if(self.totalRounds <= 1 ):
            no_servers = 1
        else:
            no_servers = int(math.log10(self.totalRounds))
        if(no_servers <= 1):
            no_servers = 1

        for x in range(no_servers):
            logging.info("Starting server%s..."%str(x))
            threading.Thread(target=self.run).start()
        t3 = threading.Thread(target=self.client).start()
        return

al = AsyncElection()

if __name__ == '__main__':
    try:
        nem = sys.argv[1]
        clie = sys.argv[2]
        ID  = sys.argv[3]
        logging.basicConfig(format='[%(levelname)s]: %(message)s',level=logging.INFO)
        al.setName(nem)
        al.setClientPort(clie)
        al.setControllerID(ID)
        al.start()
        signal.pause()
    except (KeyboardInterrupt, SystemExit):
        print "EXIT!"
        os._exit(0)
