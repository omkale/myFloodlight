package net.floodlightcontroller.hasupport;

import java.util.List;
import org.json.*;

public interface ISyncAdapter {
	
	public void packJSON(List<JSONObject> updates);

}
