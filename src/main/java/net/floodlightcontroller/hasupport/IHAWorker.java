package net.floodlightcontroller.hasupport;


import java.util.List;

import org.json.JSONObject;

import net.floodlightcontroller.core.module.IFloodlightModule;

public interface IHAWorker extends IFloodlightModule {
	
	public JSONObject getJSONObject(String controllerID);
	
	public List<String> assembleUpdate();
	
	public boolean publishHook();
	
	public boolean subscribeHook(String controllerID);
	
	

}
