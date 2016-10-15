package net.floodlightcontroller.hasupport.linkdiscovery;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.hasupport.IHAWorker;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.threadpool.IThreadPoolService;


public class LDHAWorker implements IHAWorker, ILDHAWorkerService, IFloodlightModule, ILinkDiscoveryListener {
	protected static Logger logger = LoggerFactory.getLogger(LDHAWorker.class);
	protected static ILinkDiscoveryService linkserv;
	protected static IFloodlightProviderService floodlightProvider;
	
	protected SingletonTask dummyTask;
	List<String> synLDUList = Collections.synchronizedList(new ArrayList<String>());
	protected static IThreadPoolService threadPoolService;
	private static final LDFilterQueue myLDFilterQueue = new LDFilterQueue(); 
	
	public LDHAWorker(){};
	
	@Override
	public JSONObject getJSONObject(String controllerId){
		return new JSONObject();
	}

	@Override
	public JSONObject assembleUpdate() {
		// TODO Auto-generated method stub
		JSONObject myJson = new JSONObject();
		Integer i=0;
		
		for(String update : synLDUList){
			String key = "field" + i.toString();
			myJson.append(key, update);
			i=i+1;
		}
		
		logger.info("MyJson: "+myJson.toString());
		return myJson;
	}


	@Override
	public boolean publishHook() {
		// TODO Auto-generated method stub
		try{
			synchronized (synLDUList){
				logger.info("Printing Update {}: ",new Object[]{synLDUList});
				myLDFilterQueue.enqueueForward(assembleUpdate());
				synLDUList.clear();
				TimeUnit.SECONDS.sleep(5);
				myLDFilterQueue.dequeueForward();
			}
			return true;
		} catch (Exception e){
			logger.info("[LDHAWorker] An exception occoured!");
			return false;
		}
	}


	@Override
	public boolean subscribeHook(String controllerID) {
		// TODO Auto-generated method stub
		return false;
	}
	
	/**
	 * This function is called by external users to getUpdates 
	 */
	@Override
	public JSONObject getUpdates() {
		// TODO Auto-generated method stub
		return null;
	}

    /**
     * This function is called by external users to push JSON objects 
     * into 
     */
	@Override
	public void pushUpdates(JSONObject update) {
		// TODO Auto-generated method stub
		
		
	}

	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) {
		// TODO Auto-generated method stub
		synchronized(synLDUList){
			//synLDUList.clear();
			for (LDUpdate update: updateList){	
				synLDUList.add(update.toString());
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
    	Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IThreadPoolService.class);
		return l;
	}
	
	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		linkserv = context.getServiceImpl(ILinkDiscoveryService.class);
		threadPoolService = context.getServiceImpl(IThreadPoolService.class);
	}
	
	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		logger = LoggerFactory.getLogger(LDHAWorker.class);
		linkserv.addListener(this);
		ScheduledExecutorService ses = threadPoolService.getScheduledExecutor();
		
		logger.info("LDHAWorker is starting...");

		// To be started by the first switch connection
		dummyTask = new SingletonTask(ses, new Runnable() {
			@Override
			public void run() {
				try {
					publishHook();
				} catch (Exception e) {
					logger.info("Exception in LDWorker.", e);
				} finally {
					dummyTask.reschedule(10, TimeUnit.SECONDS);	
				}
			}
		});
		dummyTask.reschedule(10, TimeUnit.SECONDS);
	}
	
}
