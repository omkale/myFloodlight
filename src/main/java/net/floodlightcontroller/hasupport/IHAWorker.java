package net.floodlightcontroller.hasupport;


import org.json.JSONObject;

import net.floodlightcontroller.core.module.IFloodlightModule;

public interface IHAWorker extends IFloodlightModule {
	
	public JSONObject getJSONObject(String controllerID);
	
	public JSONObject assembleUpdate();
	
	public boolean publishHook();
	
	public boolean subscribeHook(String controllerID);
	
	

}
