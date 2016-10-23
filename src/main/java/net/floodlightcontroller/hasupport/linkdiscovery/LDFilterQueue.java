package net.floodlightcontroller.hasupport.linkdiscovery;


import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.hasupport.IFilterQueue;

public class LDFilterQueue implements IFilterQueue {
	
	protected static Logger logger = LoggerFactory.getLogger(LDFilterQueue.class);
	private static final LDSyncAdapter syncAdapter = new LDSyncAdapter();
	
	LinkedBlockingQueue<String> filterQueue = new LinkedBlockingQueue<>();
	HashMap<String, String> myMap = new HashMap<String, String>();
	

	@Override
	public boolean enqueueForward(String value) {
		// TODO Auto-generated method stub
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(value.getBytes());
			byte[] digest = m.digest();
			BigInteger bigInt = new BigInteger(1,digest);
			String md5 = bigInt.toString(16);
			logger.info("[FilterQ] The MD5: {} The Value {}", new Object [] {md5,value});
			if( (!myMap.containsKey(md5)) && (!value.equals(null)) ){
				filterQueue.offer(value);
				myMap.put(md5, value);
			}
			return true;
	
		} 
		catch (java.security.NoSuchAlgorithmException e1) {
            e1.printStackTrace();
            return false;
        }
		catch (Exception e){
			logger.info("[FilterQ] Exception: enqueueFwd!");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean dequeueForward() {
		// TODO Auto-generated method stub
		try {
			ArrayList<String> LDupds = new ArrayList<String>();
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
