package net.floodlightcontroller.hasupport.linkdiscovery;

import org.json.JSONObject;

import net.floodlightcontroller.core.module.IFloodlightService;

public interface ILDHAWorkerService extends IFloodlightService {
	
	
	public JSONObject getUpdates();
	
	public void pushUpdates(JSONObject update);
	
	

}
