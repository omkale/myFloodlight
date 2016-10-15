package net.floodlightcontroller.hasupport;

import java.util.Map;

/**
 * This interface acts as an abstraction between the actual
 * election algorithm used by the HAController in order to
 * pick a leader; and the underlying network topology that
 * connects all the nodes together. If the network needs to be
 * modified, the methods in this interface need to be implemented
 * in order for the election algorithm to work. The connectDict  
 * mentioned here keeps state of the underlying network
 * connections between the nodes (ON/OFF). Depending on this, the 
 * election algorithm can decide whether to include this node 
 * in the election process. 
 * @author Bhargav Srinivasan, Om Kale
 *
 */

public interface NetworkInterface {
	
	/**
	 * Holds the state of the network connection for
	 * a particular node in the server configuration.
	 */
	
	public enum netState{ON,OFF};
	public enum ElectionState{CONNECT,ELECT,COORDINATE,SPIN};
	
	/**
	 * This is the send function which is used to send a message from 
	 * one node to another using the network.
	 * 
	 * @param clientPort : Destination client port.
	 * @param message    : Message that needs to be sent. 
	 * @return			 : Return code, success/fail.
	 */
	
	public Boolean send(Integer clientPort, String message);
	
	/**
	 * This is the recv() function which is used to receive a message
	 * from any other node.
	 * 
	 * @param receivingPort : Port that is waiting to receive a message.
	 * @return			    : Message that was received.
	 */
	
	public String recv(Integer receivingPort);
	
	/**
	 * This is the connectClients() function which is used to TRY connecting to
	 * all the client nodes currently present in the connectSet and store the 
	 * successfully connected clients in a dictionary called connectDict.
	 * 
	 * connectSet : A set that holds a list of the configured nodes 
	 * 			    from the server configuration
	 * @return    : An Unmodifiable copy of the HashMap which 
	 * 				holds the <portnumber, state> of all the nodes. (connectDict)
	 */
	
	public Map<Integer, netState> connectClients();
	
	/**
	 * This function is used to TRY connecting to the nodes that are 
	 * not yet connected but are present in the server configuration.
	 * It updates the connectDict to reflect the current state of the underlying network.
	 * 
	 * connectDict : HashMap which holds the <portnumber, state> of
	 * 			     all the nodes.
	 * @return     : Updated unmodifiable copy of the connectDict
	 */
	
	public Map<Integer, netState> checkForNewConnections();
	
	/**
	 * This function is used to test if the connections in the connectDict are
	 * still active and expire stale connections from the connectDict.
	 * 
	 * connectDict : HashMap which holds the <portnumber, state> of
	 * 			     all the nodes. 
	 * @return     : Updated unmodifiable copy of the connectDict
	 */
	
	public Map<Integer, netState> expireOldConnections();
	
	/**
	 * This function is used to spin in the CONNECT state of the election algorithm,
	 * until a majority (>51%) of the nodes are connected, the algorithm is locked in
	 * this state until the majority condition is satisfied.
	 */
	
	public ElectionState blockUntilConnected();
	
	/**
	 * Translate the socketDict into the connectDict, meaning set ON/OFF
	 * for the client ports that are connected, so that the election algorithm
	 * knows if it can send a message to the particular client or not.
	 */
	
	public void updateConnectDict();
	
	/**
	 * Get an unmodifiable version of the connectDict.
	 * @return : Updated unmodifiable copy of the connectDict
	 */
	
	public Map<Integer, netState> getConnectDict();
	
}
