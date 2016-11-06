package net.floodlightcontroller.hasupport.linkdiscovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.floodlightcontroller.hasupport.IFilterQueue;

/**
 * A Queue to store LDupdates
 * @author Om Kale
 */

public class LDFilterQueue implements IFilterQueue {
	
	protected static Logger logger = LoggerFactory.getLogger(LDFilterQueue.class);
	private static final LDSyncAdapter syncAdapter = new LDSyncAdapter();
	
	LinkedBlockingQueue<String> filterQueue = new LinkedBlockingQueue<>();
	HashMap<String, String> myMap = new HashMap<String, String>();
	private String newMD5 = new String();
    
	
	
	/**
	 * This function hashes the LDupdates received in form of json string 
	 * using md5 hashing and store them in the filter queue and in a map 
	 * if not already present
	 */	 
	
	@Override
	public boolean enqueueForward(String value) {
		try {		
			LDHAUtils myMD5 = new LDHAUtils();
			newMD5 = myMD5.calculateMD5Hash(value);
			logger.info("[FilterQ] The MD5: {} The Value {}", new Object [] {newMD5,value});
			if( (!myMap.containsKey(newMD5)) && (!value.equals(null)) ){
				filterQueue.offer(value);
				myMap.put(newMD5, value);
			}
			return true;
	
		} 
		catch (Exception e){
			logger.info("[FilterQ] Exception: enqueueFwd!");
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * This function pushes the LDupdates from the filter 
	 * queue into the syncAdapter
	 */
	
	
	@Override
	public boolean dequeueForward() {
		// TODO Auto-generated method stub
		try {
			ArrayList<String> LDupds = new ArrayList<String>();
			if(! filterQueue.isEmpty() ) {
				filterQueue.drainTo(LDupds);
			}
			if(! LDupds.isEmpty() ) {
				logger.info("[FilterQ] The update after drain: {} ", new Object [] {LDupds.toString()});
				syncAdapter.packJSON(LDupds);
				return true;
			} else {
				logger.info("The linked list is empty");
				return false;
			}	
		} catch (Exception e){
			logger.info("[FilterQ] Dequeue Forward failed!");
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public boolean enqueueReverse(String value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dequeueReverse() {
		// TODO Auto-generated method stub
		return false;
	}

	

}
