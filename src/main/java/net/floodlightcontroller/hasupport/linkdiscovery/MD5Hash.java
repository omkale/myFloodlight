package net.floodlightcontroller.hasupport.linkdiscovery;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Hash {
	
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
