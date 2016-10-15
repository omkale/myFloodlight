package net.floodlightcontroller.hasupport;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import net.floodlightcontroller.hasupport.NetworkInterface.netState;

/**
 * The Election class 
 * @author bsriniva
 *
 */

public class AsyncElection implements Runnable{
	
	private static Logger logger = LoggerFactory.getLogger(AsyncElection.class);
	public ZMQNode network = new ZMQNode(4242,5252,"1");
	
	/**
	 * Receiving values from the other nodes during an election.
	 */
	
	String rcvdVal = new String();
	String IWon    = new String();
	
	/**
	 * Indicates who the current leader of the entire system is.
	 */
	
	String leader  = new String();
	String tempLeader = new String();
	
	public enum ElectionState{CONNECT,ELECT,COORDINATE,SPIN};
	public ElectionState currentState = ElectionState.CONNECT;
	
	/**
	 * The connectionDict gives us the status of the nodes which are currently ON/OFF,
	 * i.e. reachable by this node or unreachable.
	 */
	
	private Map<Integer, netState> connectionDict = new HashMap<Integer, netState>();
	
	/**
	 * Standardized sleep time for spinning in the rest state.
	 */
	
	public final Integer chill = new Integer(5);
	
	public AsyncElection(){
		
	}
	
	private void checkForLeader(){
		
	}
	
	private void elect(){
		
	}
	
	private void cases(){
		try {
		while(1 == 1){
			logger.info("Current State: "+currentState.toString());
			switch(currentState){
				
				case CONNECT:
					
					// Block until a majority of the servers have connected.
					network.blockUntilConnected();
					
					// Majority of the servers have connected, moving on to elect.
					break;
					
				case ELECT:
					
					// Check for new nodes to connect to, and refresh the socket connections.
					network.checkForNewConnections();
					this.connectionDict = network.getConnectDict();
					
					// Ensure that a majority of nodes have connected, otherwise demote state.
					if(this.connectionDict.size() < network.majority){
						this.currentState = ElectionState.CONNECT;
						break;
					}
					
					//Start the election if a majority of nodes have connected.
					this.elect();
					
					// Once the election is done and the leader is confirmed,
					// proceed to the COORDINATE or FOLLOW state.
					break;
					
				case SPIN:
					
					// This is the resting state after the election.
					network.checkForNewConnections();
					if(this.leader.equals(null)){
						this.currentState = ElectionState.ELECT;
						break;
					}
					
					// This is the follower state, currently there is a leader in the network.
					logger.info("+++++++++++++++ [FOLLOWER] Leader is set to: "+this.leader.toString());
					
					// Check For Leader: This function ensures that there is only one leader set for
					// the entire network. None or multiple leaders causes it to set the currentState to ELECT.
					this.checkForLeader();
					TimeUnit.SECONDS.sleep(this.chill.intValue());
					
					break;
					
				case COORDINATE:
					
					// This is the resting state of the leader after the election.
					network.checkForNewConnections();
					if(this.leader.equals(null)){
						this.currentState = ElectionState.ELECT;
						break;
					}
					
					// This is the follower state, currently I am the leader of the network.
					logger.info("+++++++++++++++ [LEADER] Leader is set to: "+this.leader.toString());
					
//					this.sendIWon();
					
					// Keep the leader in coordinate state.
//					this.sendLeaderMsg();
//					this.setAsLeader();
					
					// Keep sending a heartbeat message, and receive a majority of acceptors,
					// otherwise go to the elect state.
//					this.sendHeartBeat();
					
					TimeUnit.SECONDS.sleep(this.chill.intValue());
					
					break;
					
			}
			
		}
		
		} catch (InterruptedException ie) {
			// TODO Auto-generated catch block
			logger.debug("[Election] Exception in cases!");
			ie.printStackTrace();
		} catch (Exception e) {
			logger.debug("[Election] Error in cases!");
			e.printStackTrace();
		}
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			Thread n1 = new Thread (network, "ZMQThread");
			n1.start();
			logger.info("[Election] Network majority: "+network.majority.toString());
			logger.info("[Election] Get connectDict: "+network.getConnectDict().toString());
				
			this.cases();
			
			n1.join();
		} catch (InterruptedException ie){
			logger.info("[Network] Was interrrupted! "+ie.toString());
			ie.printStackTrace();
		} catch (Exception e){
			logger.info("[Network] Was interrrupted! "+e.toString());
			e.printStackTrace();
		}
	}

}
