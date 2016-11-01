package net.floodlightcontroller.hasupport.linkdiscovery;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
public class LDHAWorker implements IHAWorker, ILDHAWorkerService, IFloodlightModule, ILinkDiscoveryListener {
	protected static Logger logger = LoggerFactory.getLogger(LDHAWorker.class);
	protected static ILinkDiscoveryService linkserv;
	protected static IFloodlightProviderService floodlightProvider;
	
	protected SingletonTask dummyTask;
	List<String> synLDUList = Collections.synchronizedList(new ArrayList<String>());
	protected static IThreadPoolService threadPoolService;
	private static final LDFilterQueue myLDFilterQueue = new LDFilterQueue(); 
	
	private final String[] fields = new String[]{"operation","src", "srcPort","dst","dstPort","latency","type"};
	
	public LDHAWorker(){};
	
	@Override
	public JSONObject getJSONObject(String controllerId){
		return new JSONObject();
	}
	
	public void parseChunk(String chunk){
		while(!chunk.equals("]]")){
			// pre
			if(chunk.startsWith("LDUpdate [")){
				chunk = chunk.substring(10, chunk.length());
			}
			logger.info("\n[Assemble Update] Chunk pre: {}", new Object[] {chunk});
			
			//process keywords	
			
			// field: operation
			if(chunk.startsWith("operation=")){
				chunk = chunk.substring(10,chunk.length());
				String op = chunk.split(",|]")[0];
				logger.info("[Assemble Update] Operation=: {}", new Object[]{op});
				chunk = chunk.substring(op.length(), chunk.length());
			}
			
			if(chunk.startsWith(", ")){
				chunk = chunk.substring(2, chunk.length());
			}
			
			logger.info("\n[Assemble Update] Chunk keywords: {}", new Object[] {chunk});
			
			// field: src
			if(chunk.startsWith("src=")){
				chunk = chunk.substring(4,chunk.length());
				String src = chunk.split(",|]")[0];
				logger.info("[Assemble Update] Src=: {}", new Object[]{src});
				chunk = chunk.substring(src.length(), chunk.length());
			}
			
			if(chunk.startsWith(", ")){
				chunk = chunk.substring(2, chunk.length());
			}
			
			logger.info("\n[Assemble Update] Chunk keywords: {}", new Object[] {chunk});
			
			// field: srcPort
			if(chunk.startsWith("srcPort=")){
				chunk = chunk.substring(8,chunk.length());
				String srcPort = chunk.split(",|]")[0];
				logger.info("[Assemble Update] SrcPort=: {}", new Object[]{srcPort});
				chunk = chunk.substring(srcPort.length(), chunk.length());
			}
			
			if(chunk.startsWith(", ")){
				chunk = chunk.substring(2, chunk.length());
			}
			
			logger.info("\n[Assemble Update] Chunk keywords: {}", new Object[] {chunk});
			
			// field: dst
			if(chunk.startsWith("dst=")){
				chunk = chunk.substring(4,chunk.length());
				String dst = chunk.split(",|]")[0];
				logger.info("[Assemble Update] Dst=: {}", new Object[]{dst});
				chunk = chunk.substring(dst.length(), chunk.length());
			}
			
			if(chunk.startsWith(", ")){
				chunk = chunk.substring(2, chunk.length());
			}
			
			logger.info("\n[Assemble Update] Chunk keywords: {}", new Object[] {chunk});
			
			// field: dstPort
			if(chunk.startsWith("dstPort=")){
				chunk = chunk.substring(8,chunk.length());
				String dstPort = chunk.split(",|]")[0];
				logger.info("[Assemble Update] DstPort=: {}", new Object[]{dstPort});
				chunk = chunk.substring(dstPort.length(), chunk.length());
			}
			
			if(chunk.startsWith(", ")){
				chunk = chunk.substring(2, chunk.length());
			}
			
			logger.info("\n[Assemble Update] Chunk keywords: {}", new Object[] {chunk});
			
			// field: latency
			if(chunk.startsWith("latency=")){
				chunk = chunk.substring(8,chunk.length());
				String latency = chunk.split(",|]")[0];
				logger.info("[Assemble Update] Latency=: {}", new Object[]{latency});
				chunk = chunk.substring(latency.length(), chunk.length());
			}
			
			if(chunk.startsWith(", ")){
				chunk = chunk.substring(2, chunk.length());
			}
			
			logger.info("\n[Assemble Update] Chunk keywords: {}", new Object[] {chunk});
			
			// field: type
			if(chunk.startsWith("type=")){
				chunk = chunk.substring(5,chunk.length());
				String type = chunk.split(",|]")[0];
				logger.info("[Assemble Update] Type=: {}", new Object[]{type});
				chunk = chunk.substring(type.length(), chunk.length());
			}
			
			if(chunk.startsWith(", ")){
				chunk = chunk.substring(2, chunk.length());
			}
			
			logger.info("\n[Assemble Update] Chunk keywords: {}", new Object[] {chunk});
			
			//post
			if(chunk.startsWith("], ")){
				chunk = chunk.substring(3, chunk.length());
			}
			logger.info("\n[Assemble Update] Chunk post: {}", new Object[] {chunk});
		}
		
		return;
		
	}
 
	/**
	 * This function is used to assemble the LDupdates into
	 * a JSON string using JSON Jackson API
	 * @return JSON string
	 */
	
	@Override
	public String assembleUpdate() {
		// TODO Auto-generated method stub
		StringBuilder jsonInString = new StringBuilder();
		ObjectMapper mapper = new ObjectMapper();
		JSONObject js = new JSONObject();
		
		String preprocess = new String (synLDUList.toString());
		// Flatten the updates and strip off leading [
		
		if(preprocess.startsWith("[")){
			preprocess = preprocess.substring(1, preprocess.length());
		}
		jsonInString.append(preprocess.toString());
		
		String chunk = new String(jsonInString.toString());
		
		parseChunk(chunk);
		
		logger.info("\n[Assemble Update] JSON String: {}", new Object[] {jsonInString});
		return jsonInString.toString();
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
     * This function is called by external users to push JSON strings
     * into the syncDB
     */
	@Override
	public void pushUpdates(String update) {
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
