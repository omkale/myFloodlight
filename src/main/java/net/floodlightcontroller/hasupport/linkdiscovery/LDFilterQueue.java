package net.floodlightcontroller.hasupport.linkdiscovery;


import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.hasupport.IFilterQueue;

public class LDFilterQueue implements IFilterQueue {
	
	protected static Logger logger = LoggerFactory.getLogger(LDFilterQueue.class);
	private static final LDSyncAdapter syncAdapter = new LDSyncAdapter();
	
	LinkedBlockingQueue<JSONObject> filterQueue = new LinkedBlockingQueue<>();
	MessageDigest mdEnc;
	HashMap<String, JSONObject> myMap = new HashMap<String, JSONObject>();
	

	@Override
	public boolean enqueueForward(JSONObject value) {
		// TODO Auto-generated method stub
		try {
			this.mdEnc = MessageDigest.getInstance("MD5");
			this.mdEnc.digest(value.toString().getBytes());
			String md5 = new BigInteger(1, this.mdEnc.digest()).toString(16);
			logger.info("[FilterQ] The MD5: {} The Value {}", new Object [] {md5,value});
			if( (!myMap.containsKey(md5)) && (!value.equals(null)) ){
				filterQueue.offer(value);
				myMap.put(md5, value);
			}
			return true;
		} catch (NoSuchAlgorithmException nae) {
			// TODO Auto-generated catch block
			logger.info("[FilterQ] No such algorithm MD5!");
			nae.printStackTrace();
			return false;
		} catch (Exception e){
			logger.info("[FilterQ] Exception: enqueueFwd!");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean dequeueForward() {
		// TODO Auto-generated method stub
		try {
			ArrayList<JSONObject> LDupds = new ArrayList<JSONObject>();
			if( !filterQueue.isEmpty() ){
				filterQueue.drainTo(LDupds);
			}
			if( !LDupds.isEmpty() ){
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
	public boolean enqueueReverse(JSONObject value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dequeueReverse() {
		// TODO Auto-generated method stub
		return false;
	}
	

}
