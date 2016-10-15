package net.floodlightcontroller.hasupport;

import org.json.JSONObject;

/**
 * Maintain a queue to filter out duplicates before pulling 
 * or pushing data into syncDB 
 * @author omkale
 *
 */
public interface IFilterQueue {
	
	public boolean enqueueForward(JSONObject value);
	
	public boolean dequeueForward();
	
	public boolean enqueueReverse(JSONObject value);
	
	public boolean dequeueReverse();
	

}
