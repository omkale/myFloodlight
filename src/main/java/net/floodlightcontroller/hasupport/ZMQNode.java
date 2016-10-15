package net.floodlightcontroller.hasupport;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.python.modules.math;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

public class ZMQNode implements NetworkInterface, Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(ZMQNode.class);
	
	private ZMQ.Context zmqcontext = ZMQ.context(10);
	
	public final String controllerID;
	public final Integer serverPort;
	public final Integer clientPort;
	
	/**
	 * The server list holds the server port IDs of all present 
	 * connections.
	 * The connectSet is a set of nodes defined in the serverlist 
	 * which are to be connected
	 */

	public LinkedList<Integer> serverList = new LinkedList<Integer>();
	public LinkedList<Integer> allServerList = new LinkedList<Integer>();
	public HashSet<Integer>    connectSet = new HashSet<Integer>();
	
	/**
	 * Holds the connection state/ socket object for each of the client
	 * connections.
	 */
	
	public HashMap<Integer, ZMQ.Socket> socketDict = new HashMap<Integer, ZMQ.Socket>();
	public HashMap<Integer, netState> connectDict = new HashMap<Integer, netState>();
	
	/**
	 * Standardized sleep times for retry connections, socket timeouts,
	 * number of pulses to send before expiring.
	 */
	
	public final Integer retryConnectionLatency   = new Integer(1000);
	public final Integer socketTimeout 		      = new Integer(500);
	public final Integer numberOfPulses		      = new Integer(1);
	public final Integer chill				      = new Integer(5);
	
	/**
	 * Majority is a variable that holds what % of servers need to be
	 * active in order for the election to start happening. Total rounds is
	 * the number of expected failures which is set to len(serverList) beacuse
	 * we assume that any/every node can fail.
	 */
	public final Integer majority;
	public final Integer totalRounds;
	
	
	/**
	 * Constructor needs both the backend and frontend ports and the serverList
	 * file which specifies a port number for each connected client. 
	 * @param serverPort
	 * @param clientPort
	 * @param controllerID
	 */

	public ZMQNode(Integer serverPort, Integer clientPort, String controllerID){
		/**
		 * The port variables needed in order to start the
		 * backend and frontend of the queue device.
		 */
		this.serverPort = serverPort;
		this.clientPort = clientPort;
		this.controllerID = controllerID;
		preStart();
		this.totalRounds = new Integer(this.connectSet.size());
		logger.info("Total Rounds: "+this.totalRounds.toString());
		if(this.totalRounds >= 2){
			this.majority = new Integer((int) math.ceil(new Double(0.51 * this.connectSet.size())));
		} else {
			this.majority = new Integer(1);
		}
		logger.info("Other Servers: "+this.connectSet.toString()+"Majority: "+this.majority);
		
	}
	
	public void preStart(){
		String filename = "src/main/resources/server2.config";
		
		try{
			FileReader configFile = new FileReader(filename);
			String line = null;
			BufferedReader br = new BufferedReader(configFile);
			
			while((line = br.readLine()) != null){
				this.serverList.add(new Integer(line.trim()));
				this.allServerList.add(new Integer(line.trim()));
			}
			
			this.serverList.remove(this.clientPort);
			this.connectSet = new HashSet<Integer>(this.serverList);
			
			br.close();
			configFile.close();
			
		} catch (FileNotFoundException e){
			logger.info("This file was not found! Please place the server config file in the right location.");	
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Override
	public Boolean send(Integer clientPort, String message) {
		// TODO Auto-generated method stub
		ZMQ.Socket clientSock = socketDict.get(clientPort);
		try{
			clientSock.send(message);
			return Boolean.TRUE;
		} catch(ZMQException ze){
			if(clientSock != null){
				clientSock.setLinger(0);
				clientSock.close();
			}
			logger.info("Send Failed: "+message+" not sent through port: "+clientPort.toString());
			ze.printStackTrace();
			return Boolean.FALSE;
		} catch(Exception e){
			if(clientSock != null){
				clientSock.setLinger(0);
				clientSock.close();
			}
			logger.info("Send Failed: "+message+" not sent through port: "+clientPort.toString());
			e.printStackTrace();
			return Boolean.FALSE;
		}
	}

	@Override
	public String recv(Integer receivingPort) {
		// TODO Auto-generated method stub
		ZMQ.Socket clientSock = socketDict.get(receivingPort);
		try{
			byte[] msg = clientSock.recv(0);
			return new String(msg);
		} catch(ZMQException ze){
			if(clientSock != null){
				clientSock.setLinger(0);
				clientSock.close();
			}
			logger.info("Recv Failed on port: "+receivingPort.toString());
			ze.printStackTrace();
			return "";
		} catch (Exception e){
			if(clientSock != null){
				clientSock.setLinger(0);
				clientSock.close();
			}
			logger.info("Recv Failed on port: "+receivingPort.toString());
			e.printStackTrace();
			return "";
		}
		
	}
	
	public void doConnect(){
		
		HashSet<Integer> diffSet 		= new HashSet<Integer>();
		HashSet<Integer> connectedNodes = new HashSet<Integer>();
		
		for(HashMap.Entry<Integer, ZMQ.Socket> entry: this.socketDict.entrySet()){
			connectedNodes.add(entry.getKey());
		}
		
		diffSet.addAll(this.connectSet);
		diffSet.removeAll(connectedNodes);
		
		logger.info("[Node] New connections to look for (ConnectSet - Connected): "+diffSet.toString());
		
		
		// Try connecting to all nodes that are in the diffSet and store the 
		// successful ones in the  socketDict.
		for (Integer client: diffSet){
			ZMQ.Socket clientSock = zmqcontext.socket(ZMQ.REQ);
			try{
				clientSock.connect("tcp://127.0.0.1:"+client.toString());
				//Set the socket timeouts for the current socket.
				clientSock.setReceiveTimeOut(this.socketTimeout);
				clientSock.setSendTimeOut(this.socketTimeout);
				
				clientSock.send("PULSE");
				byte[] rep = clientSock.recv();
				String reply = new String(rep);
				if(reply.equals(new String("ACK"))){
					logger.info("[Node] Client: "+client.toString()+"Client Sock: "+clientSock.toString());
					if (!socketDict.containsKey(client)){
						socketDict.put(client, clientSock);
					} else {
						logger.info("[Node] This socket already exists, refreshing: "+client.toString());
						this.socketDict.get(client).setLinger(0);
						this.socketDict.get(client).close();
						this.socketDict.remove(client);
						this.socketDict.put(client, clientSock);
					}
				} else {
					logger.info("[Node] Received bad reply: "+client.toString());
					clientSock.setLinger(0);
					clientSock.close();
					logger.info("[Node] Closed Socket"+client.toString());		
				}
				
			} catch(NullPointerException ne){
				logger.info("[Node] ConnectClients: Reply had a null value from: "+client.toString());
				if(clientSock != null){
					clientSock.setLinger(0);
					clientSock.close();
				}
				//ne.printStackTrace();
			}  catch (ZMQException ze){
				if(clientSock != null){
					clientSock.setLinger(0);
					clientSock.close();
				}
				logger.info("[Node] ConnectClients errored out: "+client.toString());
				ze.printStackTrace();
			} catch (Exception e){
				if(clientSock != null){
					clientSock.setLinger(0);
					clientSock.close();
				}
				logger.info("[Node] ConnectClients errored out: "+client.toString());
				e.printStackTrace();
			} 
			
		}
		
		return;
		
	}

	@Override
	public Map<Integer, netState> connectClients() {
		// TODO Auto-generated method stub
		logger.info("[Node] To Connect: "+this.connectSet.toString());
		logger.info("[Node] Connected: "+this.socketDict.keySet().toString());
		
		doConnect();
		
		//Delete the already connected connections from the ToConnect Set.
		for(HashMap.Entry<Integer, ZMQ.Socket> entry: this.socketDict.entrySet()){
			if(this.connectSet.contains(entry.getKey())){
				this.connectSet.remove(entry.getKey());
				logger.info("Discarding already connected client: "+entry.getKey().toString());
			}
		}	
		updateConnectDict();
		return (Map<Integer, netState>) Collections.unmodifiableMap(this.connectDict);
	}

	@Override
	public Map<Integer, netState> checkForNewConnections() {
		// TODO Auto-generated method stub
		this.connectSet = new HashSet<Integer> (this.serverList);
		
		doConnect();
		
		updateConnectDict();
		return (Map<Integer, netState>) Collections.unmodifiableMap(this.connectDict);
	}

	@Override
	public Map<Integer, netState> expireOldConnections() {
		// TODO Auto-generated method stub
		logger.info("Expiring old connections...");
		HashMap<Integer, ZMQ.Socket> delmark = new HashMap<Integer,ZMQ.Socket>();
		
		for(HashMap.Entry<Integer, ZMQ.Socket> entry: this.socketDict.entrySet()){
			try{
				byte[] rep = null;
				for(int i=0; i <= this.numberOfPulses; i++){
					entry.getValue().send("PULSE");
					rep = entry.getValue().recv();
				}
				String reply = new String(rep);
				
				if (reply != "ACK"){
					logger.info("[Node] Closing stale connection: "+entry.getKey().toString());
					entry.getValue().setLinger(0);
					entry.getValue().close();
					delmark.put(entry.getKey(),entry.getValue());
				}
				
			} catch(NullPointerException ne){
				logger.info("[Node] Expire: Reply had a null value: "+entry.getKey().toString());
				if(entry.getValue() != null){
					entry.getValue().setLinger(0);
					entry.getValue().close();
					delmark.put(entry.getKey(),entry.getValue());
				}
				//ne.printStackTrace();
			} catch(ZMQException ze){
				logger.info("[Node] Expire: ZMQ socket error: "+entry.getKey().toString());
				if(entry.getValue() != null){
					entry.getValue().setLinger(0);
					entry.getValue().close();
					delmark.put(entry.getKey(),entry.getValue());
				}
				ze.printStackTrace();
			} catch (Exception e){
				logger.info("[Node] Expire: Exception! : "+entry.getKey().toString());
				if(entry.getValue() != null){
					entry.getValue().setLinger(0);
					entry.getValue().close();
					delmark.put(entry.getKey(),entry.getValue());
				}
				e.printStackTrace();
			}
		}
		
		//Pop out all the expired connections from socketDict.
		for (HashMap.Entry<Integer, ZMQ.Socket> entry: delmark.entrySet()){
			this.socketDict.remove(entry.getKey());
		}
		
		logger.info("Expired old connections.");
		
		updateConnectDict();
		return (Map<Integer, netState>) Collections.unmodifiableMap(this.connectDict);
	}

	/**
	 * Will first expire all connections in the socketDict and keep spinning until,
	 * > majority % nodes from the connectSet get connected.
	 */
	@Override
	public ElectionState blockUntilConnected() {
		// TODO Auto-generated method stub
		this.connectSet = new HashSet<Integer> (this.serverList);
		HashMap<Integer, ZMQ.Socket> delmark = new HashMap<Integer,ZMQ.Socket>();
		
		for (HashMap.Entry<Integer,ZMQ.Socket> entry: this.socketDict.entrySet()){
			try{
				logger.info("[Node] Closing connection: "+entry.getKey().toString());
				entry.getValue().setLinger(0);
				entry.getValue().close();
				delmark.put(entry.getKey(), entry.getValue());
				
			} catch(NullPointerException ne){
				logger.info("[Node] BlockUntil: Reply had a null value"+entry.getKey().toString());
				if(entry.getValue() != null){
					entry.getValue().setLinger(0);
					entry.getValue().close();
					delmark.put(entry.getKey(),entry.getValue());
				}
				//ne.printStackTrace();
			} catch (ZMQException ze){
				logger.info("[Node] Error closing connection: "+entry.getKey().toString());
				if(entry.getValue() != null){
					entry.getValue().setLinger(0);
					entry.getValue().close();
					delmark.put(entry.getKey(),entry.getValue());
				}
				ze.printStackTrace();
			} catch (Exception e){
				logger.info("[Node] Error closing connection: "+entry.getKey().toString());
				if(entry.getValue() != null){
					entry.getValue().setLinger(0);
					entry.getValue().close();
					delmark.put(entry.getKey(),entry.getValue());
				}
				e.printStackTrace();
			}
		}
		
		for (HashMap.Entry<Integer, ZMQ.Socket> entry: delmark.entrySet()){
			this.socketDict.remove(entry.getKey());
		}
		
		this.socketDict = new HashMap<Integer, ZMQ.Socket>();
		
		while (this.socketDict.size() < this.majority){
			try {
				logger.info("[Node] BlockUntil: Trying to connect...");
				this.connectClients();
				Thread.sleep(this.retryConnectionLatency.intValue());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.info("[Node] BlockUntil connected was interrrupted!");
				e.printStackTrace();
			} catch (Exception e){
				logger.info("[Node] BlockUntil errored out: "+e.toString());
				e.printStackTrace();
			}
		}
		
		updateConnectDict();
		return ElectionState.ELECT;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			logger.info("Server List: "+this.serverList.toString());
			Thread t1 = new Thread(new QueueDevice(this.serverPort,this.clientPort), "QueueDeviceThread");
			t1.start();
			t1.join();
		} catch (InterruptedException ie){
			logger.info("Queue Device was interrupted! "+ie.toString());
			ie.printStackTrace();
		} catch (Exception e){
			logger.info("Queue Device encountered an exception! "+e.toString());
			e.printStackTrace();
		}
	}
	
	public Map<Integer, netState> getConnectDict(){
		return (Map<Integer, netState>) Collections.unmodifiableMap(this.connectDict);
	}

	/**
	 * This function translates socketDict into connectDict, in order to 
	 * preserve the abstraction of the underlying network from the actual 
	 * election algorithm.
	 */
	
	@Override
	public void updateConnectDict() {
		// TODO Auto-generated method stub
		this.connectDict = new HashMap<Integer, netState>();
		
		for (Integer seten: this.connectSet){
			this.connectDict.put(seten, netState.OFF);
		}
		
		for (HashMap.Entry<Integer, ZMQ.Socket> entry: this.socketDict.entrySet()){
			this.connectDict.put(entry.getKey(), netState.ON);
		}
		
	}

}
