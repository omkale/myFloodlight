package net.floodlightcontroller.hasupport.linkdiscovery;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

/**
 * This is the Worker class used to publish, subscribe updates to
 * and from the controller respectively
 * @author Om Kale
 *
 */
public class LDHAWorker implements IHAWorker, IFloodlightModule, ILinkDiscoveryListener {
	protected static Logger logger = LoggerFactory.getLogger(LDHAWorker.class);
	protected static ILinkDiscoveryService linkserv;
	protected static IFloodlightProviderService floodlightProvider;
	
	protected SingletonTask dummyTask;
	List<String> synLDUList = Collections.synchronizedList(new ArrayList<String>());
	protected static IThreadPoolService threadPoolService;
	private static final LDFilterQueue myLDFilterQueue = new LDFilterQueue(); 
	
	public LDHAWorker(){};
	
	/**
	 * This function is used to assemble the LDupdates into
	 * a JSON string using JSON Jackson API
	 * @return JSON string
	 */
	
	@Override
	public List<String> assembleUpdate() {
		// TODO Auto-generated method stub
		List<String> jsonInString = new LinkedList<String>();
		LDHAUtils parser = new LDHAUtils();
		
		String preprocess = new String (synLDUList.toString());
		// Flatten the updates and strip off leading [
		
		if(preprocess.startsWith("[")){
			preprocess = preprocess.substring(1, preprocess.length());
		}
		
		String chunk = new String(preprocess.toString());
		
		if(! preprocess.startsWith("]") ) {
			jsonInString = parser.parseChunk(chunk);
		}

		logger.info("\n[Assemble Update] JSON String: {}", new Object[] {jsonInString});
		return jsonInString;
	}

    /**
     * This function is called in order to start pushing updates 
     * into the syncDB
     */
	@Override
	public boolean publishHook() {
		// TODO Auto-generated method stub
		try{
			synchronized (synLDUList){
				logger.info("Printing Updates {}: ",new Object[]{synLDUList});
				List<String> updates = assembleUpdate();
				for(String update : updates){
					myLDFilterQueue.enqueueForward(update);
				}
				synLDUList.clear();
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
					subscribeHook(new String("C1"));
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
