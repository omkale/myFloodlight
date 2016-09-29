import logging
import sys
import threading
import time
import zmq
import os
import signal

class Message(object):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message

    def getMessage(self, typee):
        if(typee == "Election"):
            return Message("Election")
        elif(typee == "OK"):
            return Message("OK")
        elif(typee == "Coordinator"):
            return Message("Coordinator")
        else:
            return Message(typee)

class States(object):
    def __init__(self):
        self.connect    = "CONNECT"
        self.elect      = "ELECT"
        self.coordinate = "COORDINATE"
        self.spin       = "SPIN"
        self.current = self.connect

class AsyncElection(object):
    def __init__(self,name,controllerID,configFile="server.config"):
        self.name = str(name)
        logging.info("Port init to: %s"%self.name)
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
        self.controllerID = controllerID

        # Receiving values from other servers during election
        self.rcvdVal = set()
        self.IWon = None

        # Indicates who the current leader of the system is
        self.leader = None
        self.tempLeader = None

        # Other servers who accept you as leader
        self.allConnected = False

        # Standardized time factor for sleeps, retry time,
        # number of pulses to try before expiring.
        self.timeoutLatency = 0.01
        self.waitLatency = 3
        self.retryConnectionLatency = 5
        self.numberOfPulses = 1
        self.chill = 5

        conf = open(configFile,'r')
        for line in conf.readlines():
            line = line.rstrip()
            self.serverList.append(line)
            self.allServerList.append(line)
        self.serverList.remove(name)

        self.connectSet = set(self.serverList)
        self.totalRounds = len(self.serverList)
        self.majority = int(round(0.51*self.totalRounds))

        message = ' : '.join(str(e) for e in self.serverList)
        logging.info("Other Servers: %s Majority: %s" % (message,self.majority))

    def sendElectionMessage(self):
        # Sends an "ELECTION" message to all processes with PIDs
        # greater than its own PID
        for client, client_sock in self.connectionDict.iteritems():
            try:
                if(client > self.name):
                    client_sock.send("ELECTION "+self.name)
                    reply = client_sock.recv()
                    logging.info("[CLIENT %s] Received ACK from server %s"%(client,reply))
                    if(reply == "OK"):
                        self.rcvdVal.add(reply)
            except Exception, e:
                logging.info("[CLIENT %s] Couldn't receive ACK from server for election"%client)

        return

    def receivedIWon(self):
        # This function is for the server thread to set the IWon
        # variable after it receives an "IWON" message.
        try:
            self.IWon = 2
        except Exception, e:
            logging.info("[SERVER] Couldn't set IWon")
        return

    def waitForElectionComplete(self):
        # Since this node received an "OK" message, it sits here
        # waiting for its IWon variable to be set to 2 so that it
        # knows that there is a co-ordinator who has been elected.
        timeout = 10
        while((not self.IWon) and (timeout>0)):
            timeout = timeout-1
            logging.info("[CLIENT %s] Waiting for election to complete..."%self.name)
            time.sleep(self.waitLatency)

        logging.info("[CLIENT] Acknowledging that IWON has been set")
        return

    def sendHeartBeat(self):
        # The leader will send a heartbeat message, and will expect a
        # reply from a majority of acceptors.
        noSet = set()
        for client, client_sock in self.connectionDict.iteritems():
            try:
                client_sock.send("HEARTBEAT "+self.name)
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
                client_sock.send("IWON "+self.name)
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
                client_sock.send("LEADER "+self.name)
                reply = client_sock.recv()
                logging.info("[CLIENT %s] Received LEADOK from server %s"%(client,reply))
                if("LEADOK" in reply):
                    leadok,port = reply.split()
                    logging.info("Acceptor: %s"%port)
                    acceptors.add(port)
            except Exception, e:
                logging.info("[CLIENT %s] Couldn't receive ACK from server for election"%client)

        if (len(acceptors) >= self.majority):
            self.leader = self.name
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
                client_sock.send("SETLEAD "+self.name)
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
                if(self.connectionDict.has_key(reply)):
                    logging.info("[CLIENT %s] Check Leader: Leader exists %s"%(client,reply))
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
        if("ELECTION" in mssg):
            election,port = mssg.split()
            setl,port = mssg.split()
            if(port < self.name):
                logging.info("[SERVER] Received an ELECTION msg with low PID, sending OK")
                reply = "OK"
            else:
                logging.info("[SERVER] Received an ELECTION msg with a higher port, sending ACK")
                reply = "ACK"
        elif("IWON" in mssg):
            self.receivedIWon()
            iwon,port = mssg.split()
            self.tempLeader = port
            logging.info("[SERVER] Received IWON message from %s"%port)
            reply = "ACK"
        elif("LEADER" in mssg):
            l,port = mssg.split()
            if(self.tempLeader == port):
                reply = "LEADOK "+self.name
            else:
                reply = "NO"
        elif("SETLEAD" in mssg):
            setl,port = mssg.split()
            if not(self.leader == self.name):
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
            if(self.leader == self.name):
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

    def elect(self):
        # Bully Election Algorithm
        # Any process P can initiate the election.
        # Start by sending "ELECTION" message to all nodes
        # with a higher process ID.
        # The ones with a higher process ID than you reply by
        # sending an "OK" message.
        # If you receive an "OK" message, then yield and let the
        # election continue and wait for receiving an "IWON" message,
        # from the winner.
        # The winner then becomes the leader, after receiving a "LEADOK"
        # message from all the other nodes, which participated in the
        # election.

        # Clear all election variables.
        self.tempLeader = None
        self.leader = None
        self.rcvdVal = set()

        # Check if actually in elect state, majority connected
        if not(self.state.current == self.state.elect):
            return

        # Node joins DURING Election:
        # TODO: Need logic for node joins during election:
        # Need to send states and ACK each other before electing.
        # End of Node joins DURING Election.

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
        # Send election message to all servers with portID higher than you,
        for rnd in xrange(self.totalRounds+1):
            self.sendElectionMessage()
            time.sleep(self.timeoutLatency)

        # If you got an "OK" message, it means you have been bullied by a
        # co-ordinator, who has a higher PID, so yield and wait for "IWON"
        if("OK" in self.rcvdVal):
            self.waitForElectionComplete()
        else:
            self.sendIWon()
            time.sleep(self.timeoutLatency)
            # This function sends the leader to COORDINATE state.
            self.sendLeaderMsg()

        if(self.leader == self.name):
            logging.info("I WON THE ELECTION!")
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
                # After majority have connected, start election
                # Ensure majority are still connected
                self.checkForNewConnections()
                if(len(self.connectionDict) < self.majority):
                    time.sleep(self.retryConnectionLatency)
                    self.state.current = self.state.connect

                self.elect()
                # Once election is done and leader is confirmed, either
                # spin or coordinate.
            elif(self.state.current == "SPIN"):
                # This is the resting state after election
                self.checkForNewConnections()
                # If you don't receive a heartbeat, go to elect

                # Logic for nodes to go into election state when
                # leader is not set, OR if multiple leaders are set.
                self.checkForLeader()
                if(self.leader == None):
                    self.state.current = self.state.elect
                time.sleep(self.chill)
            elif(self.state.current == "COORDINATE"):
                # This is the resting state after election
                self.checkForNewConnections()
                self.sendIWon()
                # Keep the leader in COORDINATE state.
                self.sendLeaderMsg()
                self.setAsLeader()
                # Keep sending heartbeat, and receive majority of acceptors
                # else go to elect state.
                self.sendHeartBeat()
                if(self.leader == None):
                    self.state.current = self.state.elect
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
                client_sock.RCVTIMEO = 3000
                client_sock.SNDTIMEO = 3000
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
                logging.info("[CLIENT %s] Exception: %s"%(client,e))
                logging.info("[CLIENT %s] Could not connect to client %s, closed socket"%(client,client))

        return

    def blockTillConnected(self):
        # Reset all variables
        self.allConnected = False
        self.connectSet = set(self.serverList)

        # Close all previous connections
        delmark = {}
        for x,v in self.connectionDict.iteritems():
            try:
                logging.info("Closing connection %s %s"%(x,v))
                v.close(linger=None)
                delmark[x] = v
            except:
                delmark[x] = v
                logging.info("Failed to close connection: %s"%x[1])

        # Pop out all the old connections from the connection map
        for oldC in iter(delmark):
            self.connectionDict.pop(oldC)


        self.connectionDict = dict()
        # Spin till you get majority
        while True:
            if(len(self.connectionDict) < self.majority):
                time.sleep(self.retryConnectionLatency)
                self.connectClients()
            else:
                self.allConnected = True
                self.state.current = self.state.elect
                break
        return

    def connectClients(self):
        logging.info("To Connect: %s"%str(self.connectSet))
        logging.info("Connected: %s"%str(self.connectionDict))

        # Try conecting to all clients in ToConnect Set and store successful ones in a Dict
        for client in self.connectSet:
            try:
                client_sock = self.contextc.socket(zmq.REQ)
                client_sock.connect("tcp://127.0.0.1:"+str(client))
                # Setting send and receive timeouts
                client_sock.RCVTIMEO = 3000
                client_sock.SNDTIMEO = 3000
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
                logging.info("[CLIENT %s] Exception: %s"%(client,e))
                logging.info("[CLIENT %s] Could not connect to client %s, closed socket"%(client,client))

        # Delete already connected clients from ToConnect Set
        for port, sock in self.connectionDict.iteritems():
            if(port in self.connectSet):
                self.connectSet.discard(port)
                logging.info("Discarding already connected port %s"%port)

        return

    def run(self):
        logging.info("Starting server...")
        server_socket = self.contextc.socket(zmq.REP)
        server_socket.bind("tcp://0.0.0.0:"+str(self.name))
        # Setting send and receive timeouts
        server_socket.RCVTIMEO = 3000
        server_socket.SNDTIMEO = 3000
        while True:
            try:
                string = server_socket.recv()
                logging.info("[SERVER] Server rcvd: %s"%string)
                reply = self.processServerMsg(string)
                server_socket.send(reply)
            except Exception, e:
                logging.info("[SERVER] %s"%e)
        return

    def client(self):
        logging.info("Connecting to clients...")
        self.cases()
        return

    def start(self):
        t1 = threading.Thread(target=self.run).start()
        t2 = threading.Thread(target=self.client).start()
        return

if __name__ == '__main__':
    try:
        nem = sys.argv[1]
        ID  = sys.argv[2]
        msg = Message("")
        logging.basicConfig(format='[%(levelname)s]: %(message)s',level=logging.INFO)
        al = AsyncElection(str(nem),ID)
        al.start()
        signal.pause()
    except (KeyboardInterrupt, SystemExit):
        print "EXIT!"
        os._exit(0)
