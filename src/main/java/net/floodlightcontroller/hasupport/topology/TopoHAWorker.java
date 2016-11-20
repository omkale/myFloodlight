package net.floodlightcontroller.hasupport.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.hasupport.IHAWorker;
import net.floodlightcontroller.hasupport.IHAWorkerService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;

/**
 * This is the Worker class used to publish, subscribe updates to
 * and from the controller respectively
 * @author Om Kale
 *
 */
public class TopoHAWorker implements IHAWorker, IFloodlightModule, ILinkDiscoveryListener, ITopologyListener {
	protected static Logger logger = LoggerFactory.getLogger(TopoHAWorker.class);
	protected static ILinkDiscoveryService linkserv; //is this needed?
	protected static ITopologyService toposerv;
	protected static IFloodlightProviderService floodlightProvider;
	protected static IHAWorkerService haworker;
	
	List<String> synTopoUList = Collections.synchronizedList(new ArrayList<String>());
	protected static IThreadPoolService threadPoolService;
	private static final TopoFilterQueue myTopoFilterQueue = new TopoFilterQueue(); 
	
	public TopoHAWorker(){};
	
	public TopoFilterQueue getFilterQ(){
		return myTopoFilterQueue;
	}
	
	/**
	 * This function is used to assemble the LDupdates into
	 * a JSON string using JSON Jackson API
	 * @return JSON string
	 */
	
	@Override
	public List<String> assembleUpdate() {
		// TODO Auto-generated method stub
		List<String> jsonInString = new LinkedList<String>();
		TopoUtils parser = new TopoUtils();
		
		String preprocess = new String (synTopoUList.toString());
		// Flatten the updates and strip off leading [
		
		if(preprocess.startsWith("[")){
			preprocess = preprocess.substring(1, preprocess.length());
		}
		
		String chunk = new String(preprocess.toString());
		
		if(! preprocess.startsWith("]") ) {
			jsonInString = parser.parseChunk(chunk);
		}

		logger.info("[Assemble Update] JSON String: {}", new Object[] {jsonInString});
		return jsonInString;
	}

    /**
     * This function is called in order to start pushing updates 
     * into the syncDB
     */
	public boolean publishHook() {
		// TODO Auto-generated method stub
		try{
			synchronized (synTopoUList){
				logger.info("[Publish] Printing Updates {}: ",new Object[]{synTopoUList});
				List<String> updates = assembleUpdate();
				for(String update : updates){
					myTopoFilterQueue.enqueueForward(update);
				}
				synTopoUList.clear();
				myTopoFilterQueue.dequeueForward();
			}
			return true;
		} catch (Exception e){
			logger.debug("[LDHAWorker] An exception occoured!");
			return false;
		}
	}

	/**
	 * This function is used to subscribe to updates from the syncDB, and 
	 * stay in sync. Can be used to unpack the updates, if needed.
	 */

	public boolean subscribeHook(String controllerID) {
		// TODO Auto-generated method stub
		try {
			List<String> updates = new ArrayList<String>();
			myTopoFilterQueue.subscribe(controllerID);
			updates = myTopoFilterQueue.dequeueReverse();
			logger.info("[Subscribe] ==================== THE UPDATES: ===================");
			for (String update: updates) {
				logger.info("Update: {}", new Object[]{update.toString()});
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) {
		// TODO Is this needed?
		/*synchronized(synTopoUList){
			//synLDUList.clear();
			for (LDUpdate update: updateList){	
				synTopoUList.add(update.toString());
			}
		}*/		
	}
	
	@Override
	public void topologyChanged(List<LDUpdate> linkUpdates) {
		// TODO Auto-generated method stub
		synchronized(synTopoUList){
			//synLDUList.clear();
			for (LDUpdate update: linkUpdates){	
				synTopoUList.add(update.toString());
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
		return null;
	}
	
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
    	Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
    	l.add(IHAWorkerService.class);
    	l.add(IThreadPoolService.class);
		return l;
	}
	
	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		linkserv = context.getServiceImpl(ILinkDiscoveryService.class);
		toposerv = context.getServiceImpl(ITopologyService.class);
		threadPoolService = context.getServiceImpl(IThreadPoolService.class);
		haworker = context.getServiceImpl(IHAWorkerService.class);
		logger.info("TopoHAWorker is init...");
	}
	
	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		logger = LoggerFactory.getLogger(TopoHAWorker.class);
		linkserv.addListener(this);
		toposerv.addListener(this);
		haworker.registerService("TopoHAWorker",this);
		logger.info("TopoHAWorker is starting...");
		
		return;
	}	
	
}
