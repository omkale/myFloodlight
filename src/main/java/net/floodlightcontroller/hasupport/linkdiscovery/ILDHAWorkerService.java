package net.floodlightcontroller.hasupport.linkdiscovery;

import net.floodlightcontroller.core.module.IFloodlightService;

public interface ILDHAWorkerService extends IFloodlightService {
	
	
	public String getUpdates();
	
	public void pushUpdates(String update);
	
	

}
