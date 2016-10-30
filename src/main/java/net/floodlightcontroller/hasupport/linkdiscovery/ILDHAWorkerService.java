package net.floodlightcontroller.hasupport.linkdiscovery;

import org.json.JSONObject;

import net.floodlightcontroller.core.module.IFloodlightService;
import java.util.concurrent.*;


public interface ILDHAWorkerService extends IFloodlightService {
	
	
	public JSONObject getUpdates();
	
	public void pushUpdates(String update);
	
	

}
