package net.floodlightcontroller.hasupport.linkdiscovery;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.sdnplatform.sync.IStoreClient;
import org.sdnplatform.sync.IStoreListener;
import org.sdnplatform.sync.ISyncService;
import org.sdnplatform.sync.ISyncService.Scope;
import org.sdnplatform.sync.error.SyncException;
import org.sdnplatform.sync.internal.rpc.IRPCListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.hasupport.ISyncAdapter;
import net.floodlightcontroller.storage.IStorageSourceService;

public class LDSyncAdapter implements ISyncAdapter, IFloodlightModule, IStoreListener<String>, IRPCListener {

	protected static Logger logger = LoggerFactory.getLogger(LDSyncAdapter.class);
	protected static ISyncService syncService;
	protected static IStoreClient<String, String> storeLD;
	protected static IFloodlightProviderService floodlightProvider;
	
	private String controllerId;
	private final String none = new String("none");
	private final String[] highfields = new String[]{"operation",  "latency", "timestamp"};
	private final String[] lowfields = new String[]{"src", "srcPort", "dst","dstPort","type"};

	ObjectMapper myMapper = new ObjectMapper();
	TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String,String>>() {};
	HashMap<String, String> newUpdateMap = new HashMap<String, String>();
	HashMap<String, String> updateMap = new HashMap<String, String>();
	String cmd5Hash = new String();
	
	
	public LDSyncAdapter(){
		this.controllerId  = new String("C1");
	}
	
	public String getCMD5Hash(String update, Map<String, String> newUpdateMap) {				
		ArrayList<String> cmd5fields = new ArrayList<String>(); 
		String cmd5 = new String();
		//check map for low freq updates
		for (String lf: lowfields){
			if (newUpdateMap.containsKey(lf)){
				cmd5fields.add(newUpdateMap.get(lf));
			}
		}
		
		//cmd5fields will contain all low freq field values; take md5 hash of all values together.
		StringBuilder md5valuesb = new StringBuilder();
		for (String t: cmd5fields){
			md5valuesb.append(t);
		}
		String md5values = new String();
		md5values = md5valuesb.toString();
		
		// updateMap.put("cmd5",hash(md5values))
		// hash(...) -> means that take md5 hash of "..." 
		
		try {
			LDHAUtils myCMD5 = new LDHAUtils();
			cmd5 = myCMD5.calculateMD5Hash(md5values);
			logger.info("[cmd5Hash] The MD5: {} The Value {}", new Object [] {cmd5,md5values.toString()}); //use md5values instead of updates.	
		} 
		catch (Exception e){
			logger.info("[cmd5Hash] Exception: enqueueFwd!");
			e.printStackTrace();
		}
		return cmd5;
	}
	
	public String appendUpdate(String oldUpdate, String newUpdate) {
		
		StringBuilder updated = new StringBuilder();
		
		updated.append(oldUpdate);
		updated.append(", ");
		updated.append(newUpdate);
	
		return updated.toString();
	
	}
	
	@Override
	public void packJSON(List<String> newUpdates) {

		//TODO: Two cases for when newUpdate cmd5 = oldUpdate cmd5 and when not.
		
			for (String up: newUpdates) {
				try {
				
				newUpdateMap = myMapper.readValue(up.toString(), typeRef);
				cmd5Hash = getCMD5Hash(up,newUpdateMap);
				
				//Make the high freq fields as lists.
				String operation = newUpdateMap.get(highfields[0]);
				String latency = newUpdateMap.get(highfields[1]);
				//Add timestamp field.
				
				Long ts = new Long(Instant.now().getEpochSecond());
				Long nano = new Long(Instant.now().getNano());
				
				newUpdateMap.put(highfields[0], operation);
				newUpdateMap.put(highfields[1], latency);
				newUpdateMap.put(highfields[2], ts.toString()+nano.toString());
				
				// Try to get previous update:
	        	String oldUpdates = storeLD.getValue(cmd5Hash.toString(), none);
				
	        	if (! oldUpdates.equals(none) ) {
	        		
	        		if(oldUpdates.isEmpty()){
	        			continue;
	        		}
		        			
	        		logger.info("+++++++++++++ Retriving old update from DB: Key:{}, Value:{} ", 
	                    new Object[] {
	                            cmd5Hash.toString(), 
	                            oldUpdates.toString()
	                        }
	                 );
				
					//parse the Json String into a Map, then query the entries.
					updateMap = myMapper.readValue(oldUpdates.toString(), typeRef);		
					
				    String oldOp = updateMap.get(highfields[0]);
				    logger.info("++++OLD OP: {}", new Object[] {oldOp});
				    String opList = appendUpdate(oldOp, newUpdateMap.get(highfields[0]) );
					updateMap.put(highfields[0], opList); //update high freq fields
					
					String oldLatency = updateMap.get(highfields[1]);
				    logger.info("++++OLD LATENCY: {}", new Object[] {oldLatency});
				    String latList = appendUpdate(oldLatency, newUpdateMap.get(highfields[1]));
					updateMap.put(highfields[1], latList); //update high freq fields
					
					String oldTimestamp = updateMap.get(highfields[2]);
					logger.info("++++OLD TS: {}", new Object[] {oldTimestamp});
					Long ts2 = new Long(Instant.now().getEpochSecond());
					Long nano2 = new Long(Instant.now().getNano());
					String tmList = appendUpdate(oldTimestamp, ts2.toString()+nano2.toString());
					updateMap.put(highfields[2], tmList);
					
					LDSyncAdapter.storeLD.put(cmd5Hash.toString(), myMapper.writeValueAsString(updateMap));
					
	        	} else {
	        		
	        		try{
	        				
	        			LDSyncAdapter.storeLD.put(cmd5Hash.toString(), myMapper.writeValueAsString(newUpdateMap));
	        			
	        			String collatedcmd5 = LDSyncAdapter.storeLD.getValue(controllerId.toString(), none);
	        			
	        			if ( collatedcmd5.equals(none) ) {
	        				collatedcmd5 = cmd5Hash;
	        				logger.info("Collated CMD5: {} ", new Object [] {collatedcmd5.toString()});
	        			} else {
	        				logger.info("================ Append update to HashMap ================");
	        				collatedcmd5 = appendUpdate(collatedcmd5, cmd5Hash);
	        			}
	        			
	        			LDSyncAdapter.storeLD.put(controllerId, collatedcmd5);
	        			
	        		} catch (SyncException se) {
	        			// TODO Auto-generated catch block
	        			logger.info("[LDSync] Exception: sync packJSON!");
	        			se.printStackTrace();
	        		} catch (Exception e) {
	        			logger.info("[LDSync] Exception: packJSON!");
	        			e.printStackTrace();
	        		}
	        	}
		
			} catch (SyncException se) {
    			// TODO Auto-generated catch block
    			logger.info("[LDSync] Exception: sync packJSON!");
    			se.printStackTrace();
    		} catch (Exception e) {
    			logger.info("[LDSync] Exception: packJSON!");
    			e.printStackTrace();
	        }
		}
		
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
        Collection<Class<? extends IFloodlightService>> l =
                new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IStorageSourceService.class);
        l.add(IFloodlightProviderService.class);
        l.add(ISyncService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		logger = LoggerFactory.getLogger(LDSyncAdapter.class);
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		syncService = context.getServiceImpl(ISyncService.class);
		controllerId = floodlightProvider.getControllerId();
        logger.info("Node Id: {}", new Object[] {controllerId});
		
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		syncService.addRPCListener(this);
		try {
            LDSyncAdapter.syncService.registerStore("LDUpdates", Scope.GLOBAL);
            
            LDSyncAdapter.storeLD = LDSyncAdapter.syncService
            		.getStoreClient("LDUpdates", 
            				String.class, 
            				String.class);
            LDSyncAdapter.storeLD.addStoreListener(this);
        } catch (SyncException e) {
            throw new FloodlightModuleException("Error while setting up sync service", e);
        }
	}

	@Override
	public void keysModified(Iterator<String> keys, org.sdnplatform.sync.IStoreListener.UpdateType type) {
		// TODO Auto-generated method stub
		while(keys.hasNext()){
	        String k = keys.next();
	        try {
	        	String val = storeLD.get(k).getValue();
				logger.info("+++++++++++++ Retriving value from DB: Key:{}, Value:{}, Type: {}", 
	                    new Object[] {
	                            k.toString(), 
	                            val.toString(), 
	                            type.name()
	                        }
	                    );
	            if(type.name().equals("REMOTE")){
	              //  String info = value;
	               // logger.info("++++++++++++++++ REMOTE: Key:{}, Value:{}", k, info);
	            }
	        } catch (SyncException e) {
	            e.printStackTrace();
	        }
	    }

		
	}

	@Override
	public void disconnectedNode(Short nodeId) {
		// TODO Auto-generated method stub
		logger.info("Node disconnected: "+nodeId.toString());
	}

	@Override
	public void connectedNode(Short nodeId) {
		// TODO Auto-generated method stub
		logger.info("Node connected: "+nodeId.toString());
	}

}
