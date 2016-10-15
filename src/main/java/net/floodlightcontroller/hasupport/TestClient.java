package net.floodlightcontroller.hasupport;

import java.lang.reflect.Field;

import org.zeromq.ZMQ;

public class TestClient {
	
	public static void setSysPath(){
		System.setProperty("java.library.path", "lib/");
		System.setProperty("java.class.path", "lib/zmq.jar");
		Field sysPathsField;
		try {
			sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
			sysPathsField.setAccessible(true);
		    sysPathsField.set(null, null);
		} catch (NoSuchFieldException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	    setSysPath();
		
		ZMQ.Context zmqcontext = ZMQ.context(1);
		
		ZMQ.Socket requester = zmqcontext.socket(ZMQ.REQ);
		requester.connect("tcp://127.0.0.1:5555");
		System.out.println("Starting client...");
		
		try{ 
			requester.send("client request".getBytes(),0);
			byte[] response = requester.recv();
			System.out.println("Server Response: " + new String(response));			
			
		} catch (Exception e){
			System.out.println(e);
			requester.close();
			zmqcontext.term();
			
		}
		

	}

}