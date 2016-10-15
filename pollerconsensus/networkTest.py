import pytest
import threading
import zmq
import logging


logging.basicConfig(format='[%(levelname)s]: %(message)s',level=logging.INFO)
try:
    clientSock = zmq.Context().socket(zmq.REQ)
    clientSock.connect("tcp://127.0.0.1:%s"%str("5252"))
except Exception, e:
    logging.info("[TEST] Could not connnect to node 5252")

#def test_creation():
#    assert isinstance(appln.contextc,zmq.Context)
#
#def test_params():
#    assert type(appln.clientPort) == str
#    assert type(appln.controllerID) == int
#    assert type(appln.name) == str
#
#def test_majority():
#    maj = int(appln.totalRounds * 0.51)
#    assert appln.majority == maj

def test_connection():
    clientSock.send("PULSE")
    reply = clientSock.recv()
    assert reply == "ACK"

def test_election_higher():
    clientSock.send("ELECTION 3")
    reply = clientSock.recv()
    assert reply == "ACK"

def test_election_lower():
    clientSock.send("ELECTION 0")
    reply = clientSock.recv()
    assert reply == "OK"

def test_set_lead():
    clientSock.send("SETLEAD 1")
    reply = clientSock.recv()
    assert reply == "NO"

def test_leadok():
    clientSock.send("LEADOK")
    reply = clientSock.recv()
    assert reply == "DONTCARE"

def test_false_leader():
    clientSock.send("IWON 1")
    reply = clientSock.recv()
    print reply
    clientSock.send("LEADER 1")
    reply = clientSock.recv()
    print reply
    clientSock.send("SETLEAD 1")
    reply = clientSock.recv()
    assert reply == "NO"

def test_heartbeat():
    clientSock.send("HEARTBEAT 1")
    reply = clientSock.recv()
    print reply
    assert reply == "ACK"

def test_bad_heartbeat():
    clientSock.send("HEARTBEAT")
    reply = clientSock.recv()
    print reply
    assert reply == 0
