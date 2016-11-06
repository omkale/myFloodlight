package net.floodlightcontroller.hasupport.linkdiscovery;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LDHAUtils {
	protected static Logger logger = LoggerFactory.getLogger(LDHAUtils.class);
	
	public List<String> parseChunk(String chunk){
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String,Object> newJson = new HashMap<String,Object>();
		List<String> jsonInString = new LinkedList<String>();
		
		String op          = new String();
		String src         = new String();
		String srcPort     = new String();
		String dst         = new String();
		String dstPort     = new String();
		String latency     = new String();
		String type        = new String();
		
		try {
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
				op = chunk.split(",|]")[0];
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
			    src = chunk.split(",|]")[0];
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
				srcPort = chunk.split(",|]")[0];
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
				dst = chunk.split(",|]")[0];
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
				dstPort = chunk.split(",|]")[0];
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
				latency = chunk.split(",|]")[0];
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
				type = chunk.split(",|]")[0];
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
			
			//TODO: Put it in a JSON.
			if(! op.isEmpty() ){
				newJson.put("operation", op);
			}
			if(! src.isEmpty() ){
				newJson.put("src", src);
			}
			if(! srcPort.isEmpty() ){
				newJson.put("srcPort", srcPort);
			}
			if(! dst.isEmpty() ){
				newJson.put("dst", dst);
			}
			if(! dstPort.isEmpty() ){
				newJson.put("dstPort", dstPort);
			}
			if(! latency.isEmpty() ){
				newJson.put("latency", latency);
			}
			if(! type.isEmpty() ){
				newJson.put("type", type);
			}
			
			try{
				jsonInString.add(mapper.writeValueAsString(newJson));
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} 
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return jsonInString;
	}
	
	public String calculateMD5Hash(String value){
		String md5 = new String();
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(value.getBytes());
			byte[] digest = m.digest();
			BigInteger bigInt = new BigInteger(1,digest);
			md5 = bigInt.toString(16);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return md5;	
	}

}
