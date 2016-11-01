package net.floodlightcontroller.hasupport.linkdiscovery;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
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
import org.sdnplatform.sync.internal.store.JacksonStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
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
	HashMap<String, String> lowFreqUpdate = new HashMap<String, String>();
	Map<String, String> updateMap = new HashMap<String, String>();
	Map<String, String> newUpdateMap = new HashMap<String, String>();
	
	private String controllerId;
	private final String[] highfields = new String[]{"operation", "latency"};
	private final String[] lowfields = new String[]{"src", "dst", "srcPort","dstPort","type"};
	private String cmd5;
	
	
	public LDSyncAdapter(){
		
	}
	
	public String getCMD5Hash(String updates) {				
//		ArrayList<String> cmd5fields = new ArrayList<String>();
		//check map for low freq updates
//		for (String lf: lowfields){
//			if (newUpdateMap.containsKey(lf)){
//				cmd5fields.add(newUpdateMap.get(lf));
//			}
//		}
//		
//		//cmd5fields will contain all low freq field values; take md5 hash of all values together.
//		StringBuilder md5valuesb = new StringBuilder();
//		for (String t: cmd5fields){
//			md5valuesb.append(t);
//		}
//		String md5values = new String();
//		md5values = md5valuesb.toString();
		
		//take md5 hash of 'md5values' and that will be your cmd5
		// updateMap.put("cmd5",hash(md5values))
		// hash(...) -> means that take md5 hash of "..." and make that the string.
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(updates.getBytes()); //use md5values instead of updates.
			byte[] digest = m.digest();
			BigInteger bigInt = new BigInteger(1,digest);
			cmd5 = bigInt.toString(16);
			logger.info("[cmd5Hash] The MD5: {} The Value {}", new Object [] {cmd5,updates}); //use md5values instead of updates.	
	
		} 
		catch (java.security.NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        }
		catch (Exception e){
			logger.info("[cmd5Hash] Exception: enqueueFwd!");
			e.printStackTrace();
		}
		return cmd5;
	}
	
	
	@Override
	public void packJSON(List<String> newUpdates) {
		ObjectMapper myObj = new ObjectMapper();
		
		//TODO: Two cases for when newUpdate cmd5 = oldUpdate cmd5 and when not.
		
		String cmd5Hash = new String();
		
		//Get previous update:
		try {
			
			cmd5Hash = getCMD5Hash(newUpdates.get(0).toString());
			
			newUpdateMap = (Map<String, String>)(myObj.readValue(newUpdates.toString(), Map.class));
			
        	String oldUpdates = storeLD.get(cmd5Hash).getValue();
			logger.info("+++++++++++++ Retriving old update from DB: Key:{}, Value:{} ", 
                    new Object[] {
                            cmd5Hash.toString(), 
                            oldUpdates.toString()
                        }
                    );
			////parse the Json String into a Map, then query the entries.
			
			updateMap = (Map<String, String>)(myObj.readValue(oldUpdates.toString(), Map.class));
		
		//latency = updateMap.get(highfields[0]); lantencyList = convertList(latency); latencyList.append("current update"); updateMap.put(highfields[0], latencyList);
		// timestamp = updateMap.get("timestamp"); timeList = convertList(timestamp); timeList.appenc(System.time()); updateMap.put("timestamp",timeList);
		
		updateMap.put("cmd5", cmd5Hash);   //lowfreq updates
		
	    String oldLatency = updateMap.get(highfields[0]);
		List<String> latencyList = Arrays.asList(oldLatency.split("\\s*,\\s*"));
		latencyList.add( newUpdateMap.get(highfields[0]) );
		updateMap.put(highfields[0], latencyList.toString()); //update high freq fields
		
		
		String timestamp = updateMap.get("timestamp");
		List<String> timeStampList = Arrays.asList(timestamp.split("\\s*,\\s*"));
		Long ts = new Long(Instant.now().getEpochSecond());
		timeStampList.add(ts.toString());
		updateMap.put("timestamp", timeStampList.toString());
		
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SyncException e) {
            e.printStackTrace();
        }
				
		try{				    
			LDSyncAdapter.storeLD.put(cmd5Hash, updateMap.toString());
			logger.info("+++++++++++++ Retrieving from DB: CID:{}, Update:{}", 
                    new Object[] {
                            controllerId,
                            updateMap.toString()
                        });
			
		} catch (SyncException se) {
			// TODO Auto-generated catch block
			logger.info("[LDSync] Exception: sync packJSON!");
			se.printStackTrace();
		} catch (Exception e) {
			logger.info("[LDSync] Exception: packJSON!");
			e.printStackTrace();
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
	            //JSONObject val = new JSONObject(serzVal);
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
