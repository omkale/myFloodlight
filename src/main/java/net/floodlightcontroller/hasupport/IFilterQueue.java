package net.floodlightcontroller.hasupport;


/**
 * Maintain a queue to filter out duplicates before pulling 
 * or pushing data into syncDB 
 * @author omkale
 *
 */
public interface IFilterQueue {
	
	public boolean enqueueForward(String value);
	
	public boolean dequeueForward();
	
	public boolean enqueueReverse(String value);
	
	public boolean dequeueReverse();
	

}
