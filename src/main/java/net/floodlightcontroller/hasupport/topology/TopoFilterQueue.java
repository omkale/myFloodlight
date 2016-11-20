package net.floodlightcontroller.hasupport.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.floodlightcontroller.hasupport.IFilterQueue;

/**
 * A Queue to store Topology updates
 * @author Om Kale
 */

public class TopoFilterQueue implements IFilterQueue {
	
	protected static Logger logger = LoggerFactory.getLogger(TopoFilterQueue.class);
	private static final TopoSyncAdapter syncAdapter = new TopoSyncAdapter();
	
	public static LinkedBlockingQueue<String> filterQueue = new LinkedBlockingQueue<>();
	public static HashMap<String, String> myMap = new HashMap<String, String>();
	
	public static LinkedBlockingQueue<String> reverseFilterQueue = new LinkedBlockingQueue<>();
	
	
	/**
	 * This function hashes the LDupdates received in form of json string 
	 * using md5 hashing and store them in the filter queue and in a map 
	 * if not already present
	 */	 
	
	@Override
	public boolean enqueueForward(String value) {
		try {
			String newMD5 = new String();
			TopoUtils myMD5 = new TopoUtils();
			newMD5 = myMD5.calculateMD5Hash(value);
			logger.debug("[FilterQ] The MD5: {} The Value {}", new Object [] {newMD5,value});
			if( (!myMap.containsKey(newMD5)) && (!value.equals(null)) ){
				filterQueue.offer(value);
				myMap.put(newMD5, value);
			}
			return true;
	
		} 
		catch (Exception e){
			logger.debug("[FilterQ] Exception: enqueueFwd!");
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
				logger.info("[FilterQ] The linked list is empty");
				return false;
			}	
		} catch (Exception e){
			logger.info("[FilterQ] Dequeue Forward failed!");
			e.printStackTrace();
		}
		
		return false;
	}
	
	@Override
	public void subscribe(String controllerID) {
		syncAdapter.unpackJSON(controllerID);
		return;
	}

	@Override
	public boolean enqueueReverse(String value) {
		// TODO Auto-generated method stub
		try {
			logger.info("[ReverseFilterQ] The Value {}", new Object [] {value});
			if( (!value.equals(null)) ){
				reverseFilterQueue.offer(value);
			}
			return true;
		} 
		catch (Exception e){
			logger.info("[ReverseFilterQ] Exception: enqueueFwd!");
			e.printStackTrace();
			return true;
		}
		
	}

	@Override
	public List<String> dequeueReverse() {
		// TODO Auto-generated method stub
		ArrayList<String> LDupds = new ArrayList<String>();
		try {
			if(! reverseFilterQueue.isEmpty() ) {
				reverseFilterQueue.drainTo(LDupds);
			}
			
			if(! LDupds.isEmpty() ) {
				logger.debug("[ReverseFilterQ] The update after drain: {} ", new Object [] {LDupds.toString()});
				return LDupds;
			} else {
				logger.info("[ReverseFilterQ] The linked list is empty");
			}
			return LDupds;
			
		} catch (Exception e){
			logger.debug("[ReverseFilterQ] Dequeue Forward failed!");
			e.printStackTrace();
		}
		
		return LDupds;
		
	}

}