package net.floodlightcontroller.hasupport;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQQueue;

public class QueueDevice implements Runnable{

	private static Logger logger = LoggerFactory.getLogger(QueueDevice.class);
	
	public final Integer serverPort;
	public final Integer clientPort;
	
	public QueueDevice(int servePort, int clienPort) {
		// TODO Auto-generated constructor stub
		this.serverPort = servePort;
		this.clientPort = clienPort;
	}


	public QueueDevice(Integer servePort,Integer clienPort){
		this.serverPort = servePort;
		this.clientPort = clienPort;
	}
	
	
	public void startQueue() {
		
		try{
			/**
			 * Number of I/O threads assigned to the queue device.
			 */
			ZMQ.Context zmqcontext = ZMQ.context(10);
			
			/** 
			 * Connection facing the outside, where other nodes can connect 
			 * to this node. (frontend)
			 */
			ZMQ.Socket clientSide = zmqcontext.socket(ZMQ.ROUTER);
			clientSide.bind("tcp://0.0.0.0:"+this.clientPort.toString());
			
			
			/**
			 * The backend of the load balancing queue and the server 
			 * which handles all the incoming requests from the frontend.
			 * (backend)
			 */
			ZMQ.Socket serverSide = zmqcontext.socket(ZMQ.DEALER);
			serverSide.bind("tcp://0.0.0.0:"+this.serverPort.toString());
			
			logger.info("Starting ZMQueue device...");
			
			/**
			 * This is an infinite loop to run the QueueDevice!
			 */
			ZMQQueue queue = new ZMQQueue(zmqcontext,clientSide,serverSide);
			queue.run();
			
			queue.close();
			clientSide.close();
			serverSide.close();
			zmqcontext.term();
			
		} catch (ZMQException ze){		
			logger.debug("Zero MQ Exception occoured "+ze.toString());
			ze.printStackTrace();	
		} catch (IOException ie){
			logger.debug("I/O exception occoured while trying to close QueueDevice "+ie.toString());
			ie.printStackTrace();
		} catch (Exception e){
			logger.debug("Exception occoured while trying to close QueueDevice "+e.toString());
			e.printStackTrace();
		}
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		startQueue();
		
	}

}
