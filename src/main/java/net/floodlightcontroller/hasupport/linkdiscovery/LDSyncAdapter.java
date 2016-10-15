package net.floodlightcontroller.hasupport.linkdiscovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.sdnplatform.sync.IStoreClient;
import org.sdnplatform.sync.IStoreListener;
import org.sdnplatform.sync.ISyncService;
import org.sdnplatform.sync.ISyncService.Scope;
import org.sdnplatform.sync.error.SyncException;
import org.sdnplatform.sync.internal.rpc.IRPCListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	protected static IStoreClient<String, JSONObject> storeLD;
	protected static IFloodlightProviderService floodlightProvider;
	private String controllerId;

	public LDSyncAdapter(){
		
	}

	@Override
	public void packJSON(List<JSONObject> updates) {
		// TODO Auto-generated method stub
		try {
			Integer i = new Integer(0);
			for (JSONObject update: updates){
				LDSyncAdapter.storeLD.put((controllerId+i.toString()), update);
				logger.info("+++++++++++++ Retrieving from DB: CID:{}, Update:{}", 
	                    new Object[] {
	                            controllerId,
	                            update
	                        });
				i = i+1;
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
            				JSONObject.class);
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
	            String value = storeLD.get(k).getValue().toString();
				logger.info("+++++++++++++ Retriving value from DB: Key:{}, Value:{}, Type: {}", 
	                    new Object[] {
	                            k, 
	                            value, 
	                            type.name()
	                        }
	                    );
	            if(type.name().equals("REMOTE")){
	                String info = value;
	                logger.info("++++++++++++++++ REMOTE: Key:{}, Value:{}", k, info);
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
