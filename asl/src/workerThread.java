package asl;

import java.util.Random;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.io.PrintWriter;
import java.io.FileWriter;


/*

This is the class representation of worker thread, while is responsible for most of the work in middleware such as parsing and processing the request, starting the logger, etc. 

Functions:

logger() -> Used to write collected data to log files
counter() -> Used for load balancing, function returns one of the server from set of server one-by-one.
run() -> This function handles most of the work of the worker thread. This is responsible to start scheduled task which run every one second and log information to the log file. This function is also responsible for parsing the request and then processing the reuqest appropriately. After sending each complete request, it waits for the response from corresponding servers, after receiving the response -> response is parsed/merged/checked (based on different  parameters, such as if the request was SET or GET or multi-GET and if the middleware is running in sharded or non-sharded mode) and then forwarded to the client. 
splitIntoParts(..) -> This function is used to calculate how to split the GET request efficiently between servers, when middleware is running in sharded mode. 


We have added inline comments, where-ever deemed necessary to understand the structure.

*/
public class workerThread implements Runnable {

	private List<String> mcAddrs;	
	private Selector selector;
	private LinkedBlockingQueue<mcRequest> requestsQueue;
	private ByteBuffer buffer = ByteBuffer.allocate(1024); 
	private ByteBuffer setBuffer = ByteBuffer.allocate(1024); 
	private boolean sharded;
	private SocketChannel channel;
	private String requestType = null;
	private ByteBuffer resp = ByteBuffer.allocate(1024);	
	private static long load_counter = 0L;
	private static int server_count = 0;
	//private static int divider = 0;
	private String serverVal = null;
	private String[] getType = null;
	//logging part
	private boolean timerStatus = false;  // To make note of timer (scheduler) status
	private int[] serverReqCount; //to count number of request sent to each server - provablility load balancer
	private int setOps = 0;
	private int getOps = 0;
	private int multiGetOps = 0;
	//get
	private double getWaitingTime = 0; // Time for which request wait in the requests queue
	private double getServiceTimeMC = 0; // Time taken by memcached server to process the request
	private double getServiceTime = 0; // Overall time taken by middleware to process the requests
	private double getResponseTime = 0; // Overall response time measured in the middleware
	
	/* Below variables are similarly used to note different measurements for multi-GET and SET operations */
	
	//multi-Get 
	private double mGetWaitingTime = 0;
	private double mGetServiceTimeMC = 0;
	private double mGetServiceTime = 0;
	private double mGetResponseTime = 0;
	private double mGetKeySize = 0; 
	//set
	private double setWaitingTime = 0;
	private double setServiceTimeMC = 0;
	private double setServiceTime = 0;
	private double setResponseTime = 0;
		
	private double queueLength = 0;
	private int missOps = 0;
	protected static ArrayList<Long> responseTimeHist = new ArrayList<Long>();  //note every response time for histogram - This is used to make note of each response time and finally dump them into a file.
	
	//Logger File
	private PrintWriter logWriter = null;
	private static int threadCounter = 0; //Used for log-file naming

	public workerThread(List<String> mcAddress, LinkedBlockingQueue<mcRequest> requestsQueue, boolean sharded) {
		this.mcAddrs = mcAddress;
		server_count = this.mcAddrs.size();
		this.requestsQueue = requestsQueue;
		this.sharded = sharded;
		this.serverReqCount = new int[this.mcAddrs.size()];
		for (int i=0; i<this.mcAddrs.size(); i++) {
			this.serverReqCount[i] = 0;
		}
		try{
			this.logWriter = new PrintWriter(new FileWriter("LogFile-"+ threadCounter +".log"));  // Create log files based on total number of wroker threads. 
			// Write the first row to file 
			this.logWriter.println("SetOps  setWaitingTime setServiceTimeMC  setServiceTime  setResponseTime  GetOps  getWaitingTime  getServiceTimeMC  getServiceTime  getResponseTime  HitOps  MissOps  mGetOps  mGetAvgSize  mGetWaitingTime  mGetServiceTimeMC  mGetServiceTime  mGetResponseTime ThroughPut  qLength  ServerReqCount");
			threadCounter++;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void logger()
	{
		int th = this.setOps+this.getOps+this.multiGetOps;
		if ((th)>0){ /* Don't start writig to file until experiment start */
		this.logWriter.println(this.setOps + "  " + (this.setWaitingTime/this.setOps) + "  "+ (this.setServiceTimeMC/this.setOps) + "  " + (this.setServiceTime/this.setOps) + "  " + (this.setResponseTime/this.setOps) + "  " + this.getOps + "  " + (this.getWaitingTime/this.getOps) + "  " + (this.getServiceTimeMC/this.getOps) + "  " + (this.getServiceTime/this.getOps) + "  " + (this.getResponseTime/this.getOps) + "  " + (this.getOps-this.missOps) + "  " + this.missOps + "  " + this.multiGetOps + "  " + (this.mGetKeySize/this.multiGetOps) +"  "+ (this.mGetWaitingTime/this.multiGetOps) + "  " + (this.mGetServiceTimeMC/this.multiGetOps) + "  " + (this.mGetServiceTime/this.multiGetOps) + "  " + (this.mGetResponseTime/this.multiGetOps) + "  " + th + "  " + (this.queueLength/th) + "  " + Arrays.toString(serverReqCount));
		this.logWriter.flush();
		}
	}
	
	public static int counter()
	{
		load_counter++;
		return (int) (load_counter%server_count);
	}
	
	//divide into parts -- mget Sharded
	private static int[] splitIntoParts(int whole, int parts) {
		int[] arr = new int[parts];
		for (int i = 0; i < arr.length; i++)
			whole -= arr[i] = (whole + parts - i - 1) / (parts - i);
		return arr;
	}


	@Override
	public void run() {		
		
		try {
			this.selector = Selector.open();
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
		// Start connections to all the memcached servers
		int mcServerCount = this.mcAddrs.size();
		SocketChannel socketChannels[] = new SocketChannel[mcServerCount];

		for (int i=0; i<mcServerCount; i++) {
			String serverAddress = mcAddrs.get(i).split(":")[0];
			int serverPort = Integer.parseInt(mcAddrs.get(i).split(":")[1]);
			try {
				socketChannels[i] = SocketChannel.open();
				socketChannels[i].configureBlocking(true); // Configure the socket channel and active the blocking
				socketChannels[i].connect(new InetSocketAddress(serverAddress, serverPort));

				while(!socketChannels[i].finishConnect() ){
					// Wait for connection to finish before proceeding to next connection
				}
			} 	
		catch (IOException e) {
			e.printStackTrace();
			}
		}   // END for loop
		
		int writeTo;  // Refers to the server number, to which we will be writing the request. 
		while (true) {
			mcRequest request = null;
			//Random randomGenerator = new Random(); // Random server to request from
			writeTo = counter(); // Call counter() function and assign the returned value to variable.
			try {
				// Take one request from the queue. If there is no request in the queue -> block waiting until an element is available.
				request = this.requestsQueue.take();
				request.setTimeDequeued(System.nanoTime()); // note the time at which request gets dequeued from the queue
				request.setTimeParseStart(System.nanoTime()); // note the time at which parsing of request starts
				//this.waitingTime += (request.getTimeDequeued() - request.getTimeEnqueued());
				this.queueLength += this.requestsQueue.size();  // note the current length of requests queue
				
				if (this.timerStatus == false) { //start scheduler, if it has not been started yet. 
				//this.timerStatus is set to True, when first request is picked up by the worker thread
					Timer timer = new Timer();
					timer.schedule(new TimerTask() {  /* Start a scheduled task */
						@Override
						public void run () {
							logger();
						}
					}, 1000, 1000);
					timerStatus = true;
				}

				byte[] requestArray = request.getRawRequest();
				requestType = new String(requestArray, 0, 4);  // Check the requestType -> GET or SET
				String requestString = new String(requestArray);
				serverVal = "";
				if (requestType.equals("get ") || requestType.equals("set "))  //check if request is get or set, else the request is invalid and don't process the request
				{
					if (requestType.equals("get ")){ // check if request is get
						getType = requestString.split(" ");
						if ((sharded) && (getType.length>2)){ // Check if request is multi-GET and middleware is running in sharded mode
							request.setTimeParseEnd(System.nanoTime()); // Note the time when parsing ends
							
							/* Calculate and record the waiting time of request. 
							 We record the information separately for each type of request. (GET, SET or mGET) */	
							 
							this.mGetWaitingTime += ((request.getTimeParseEnd() - request.getTimeEnqueued()) - (request.getTimeParseEnd() - request.getTimeParseStart()));   
							this.multiGetOps++; // Used to note the number of mGET operation
							int keyCount = getType.length - 1; // Because first one refers to request type here.
							this.mGetKeySize += keyCount; // Record the key Size of every multi-GET operation
							getType[keyCount] = getType[keyCount].replace("\r\n", ""); // Remove control charcters from request 
							request.setTimeSentToServer(System.nanoTime());
						
								// Multi Get - Sharded
								
								// Call function "splitIntoParts" to find how to efficiently split the GET-request between set of memcached servers
								int [] parts = splitIntoParts(keyCount, mcServerCount); int j=1, part = 0;
								part = parts[0];
								for (int i=0; i<parts.length; i++)
								{									
									this.buffer.clear();
									String partialRequest = requestType;
									while (j<=part)
									{	partialRequest += getType[j] + ' '; j++; }
									this.buffer.put((partialRequest +"\r\n").getBytes());  
									this.buffer.flip();
									while (this.buffer.hasRemaining()) {
										socketChannels[i].write(this.buffer);  // 'i' acts as serverNumber
										serverReqCount[i] += 1;
									}
									serverVal += i; 
									if (part<keyCount) {part+=parts[i+1];}
								}
								
							}
						//END multi GET - Sharded
						else if ((sharded == false) && (getType.length>2)){ // If the request is multi-get and middleware is running in non sharded mode
							request.setTimeParseEnd(System.nanoTime());
							this.mGetWaitingTime += ((request.getTimeParseEnd() - request.getTimeEnqueued()) - (request.getTimeParseEnd() - request.getTimeParseStart()));   
							// Exact waiting time request spent in queue - categorized by Request
							this.multiGetOps++;
							int keyCount = getType.length - 1; // Because one of them is request type itself
							this.mGetKeySize += keyCount; 
							this.buffer.clear();	
							this.buffer.put(request.getRawRequest());
							this.buffer.flip();
							request.setTimeSentToServer(System.nanoTime());
							serverReqCount[writeTo] += 1;  // Record the number of reuests sent to a particular server. Used to test load balancer. 
							while (this.buffer.hasRemaining()) {
								socketChannels[writeTo].write(this.buffer);
							}
						}
						else{ // normal get request
							request.setTimeParseEnd(System.nanoTime());
							this.getWaitingTime += ((request.getTimeParseEnd() - request.getTimeEnqueued()) - (request.getTimeParseEnd() - request.getTimeParseStart()));   
							this.getOps++;
							this.buffer.clear();	
							this.buffer.put(request.getRawRequest());
							this.buffer.flip();
							request.setTimeSentToServer(System.nanoTime());
							serverReqCount[writeTo] += 1;
							while (this.buffer.hasRemaining()) {
								socketChannels[writeTo].write(this.buffer);
							}
						}
					}
					else{ // if the request is a write operation
						
						request.setTimeParseEnd(System.nanoTime());
						this.setWaitingTime += ((request.getTimeParseEnd() - request.getTimeEnqueued()) - (request.getTimeParseEnd() - request.getTimeParseStart()));   
						this.setOps++; //logging
						request.setTimeSentToServer(System.nanoTime());
						for (int i=0; i<mcServerCount; i++) {

							this.buffer.clear();
							this.buffer.put(request.getRawRequest());
							this.buffer.flip();
							while(this.buffer.hasRemaining()) {
								socketChannels[i].write(this.buffer);   // Send the SET request to every connected memcached server for the replication purpose
							}	
							this.buffer.rewind();
						}
						this.buffer.clear();
					}
				}
				else  // This is an invalid request
				{
					//System.out.println("Invalid Request");
					continue;
				}
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (IndexOutOfBoundsException e) { 
				//System.out.println("Invalid Request"); //if client send small request than assumed
			}
			
			/* Read response from the corresponding servers and then apply operation on them as if requeired, 
			after which forward the response back to the client from where request came */
			
			if (requestType.equals("get ") && (sharded) && (getType.length>2))  // If the request was multi-GET and middleware is running in sharded mode
			{
				ByteBuffer buf = ByteBuffer.allocate(2048); 
				buf.clear();
				for (int i =0; i<serverVal.length(); i++){
					try {
						socketChannels[Character.getNumericValue(serverVal.charAt(i))].read(buf); //Read from all socket channels where we sent splitted request
						
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
				try{
				buf.flip();
				String responseStr = new String(buf.array(), "ASCII"); 
				//System.out.println(responseStr);
				String responses[] = responseStr.split("\r\n");
				String finalResp = null; finalResp="";
				
				/* Combine the responses from all memcached servers */
				
				for (int i=0; i<responses.length-1; i++) {  // extra "\n" in last value, therefore -1
					if (responses[i].equals("END")){} else{
					finalResp+=responses[i]+"\r\n";
					}
				}
				
				// Record different type of measurements 
                request.setTimeReceivedFromServer(System.nanoTime());
				this.mGetServiceTimeMC += (request.getTimeReceivedFromServer() - request.getTimeSentToServer());
				this.mGetServiceTime += (request.getTimeReceivedFromServer() - request.getTimeDequeued());
				this.mGetResponseTime += (request.getTimeReceivedFromServer() - request.getTimeEnqueued());
				responseTimeHist.add(request.getTimeReceivedFromServer() - request.getTimeEnqueued());
				resp.clear();
				resp.put((finalResp+"END\r\n").getBytes());
				resp.flip();

				//Send response back to the client using the same socket channel.
				while (this.resp.hasRemaining()) {
					try {
						request.getSocketChannel().write(this.resp);  
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				buf.clear();
				this.resp.clear();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
				
			} else if (requestType.equals("get ") && (getType.length>2) && (sharded == false)){  // Multi-Get -- Non-Sharded 
			try {
				this.buffer.clear();
				socketChannels[writeTo].read(this.buffer); //read from the channel where you sent request

                request.setTimeReceivedFromServer(System.nanoTime());
				this.mGetServiceTimeMC += (request.getTimeReceivedFromServer() - request.getTimeSentToServer());
				this.mGetServiceTime += (request.getTimeReceivedFromServer() - request.getTimeDequeued());
				this.mGetResponseTime += (request.getTimeReceivedFromServer() - request.getTimeEnqueued());
				responseTimeHist.add(request.getTimeReceivedFromServer() - request.getTimeEnqueued());
				this.buffer.flip();  //ready to read

				//Send response back to the client using the same socket channel.
				while (this.buffer.hasRemaining()) {
					try {
						request.getSocketChannel().write(this.buffer);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				}
			}
			else if (requestType.equals("get ") && (getType.length==2) ){ // GET request
			try {
				this.buffer.clear();
				socketChannels[writeTo].read(this.buffer); //read from the channel where you sent request

                request.setTimeReceivedFromServer(System.nanoTime());
				this.getServiceTimeMC += (request.getTimeReceivedFromServer() - request.getTimeSentToServer());
				this.getServiceTime += (request.getTimeReceivedFromServer() - request.getTimeDequeued());
				this.getResponseTime += (request.getTimeReceivedFromServer() - request.getTimeEnqueued());
				responseTimeHist.add(request.getTimeReceivedFromServer() - request.getTimeEnqueued());
				this.buffer.flip(); 

				// If buffer only contains END\r\n the value couldn't be found -> get miss.
				if (this.buffer.remaining() == 5) {
					this.missOps++;  // Read operation was a miss operation
				}

				//Send response back to the client using the same socket channel.
				while (this.buffer.hasRemaining()) {
					try {
						request.getSocketChannel().write(this.buffer);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			}
			else if (requestType.equals("set ")){   // SET request

				this.setBuffer.clear();
				for (int i=0; i<mcServerCount; i++) {
					try {
						socketChannels[i].read(this.setBuffer); //read from the socket channels where you sent request.
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
				this.setBuffer.flip();
				
				try{
					// Parse Response
					String setresponseStr = new String(this.setBuffer.array(), "ASCII");
					String responses[] = setresponseStr.split("\r\n");
					
					for (int i=0; i<responses.length-1; i++) { 

						if (responses[i].equals("STORED")) {
							request.setResponseMsg(responses[i]); //save response
						}
						else {
							// save the error to send it back.
							request.setResponseMsg(responses[i]);
							break;
						}
					}
					
					request.setTimeReceivedFromServer(System.nanoTime());
					this.setServiceTimeMC += (request.getTimeReceivedFromServer() - request.getTimeSentToServer());
					this.setServiceTime += (request.getTimeReceivedFromServer() - request.getTimeDequeued());
					this.setResponseTime += (request.getTimeReceivedFromServer() - request.getTimeEnqueued());
					responseTimeHist.add(request.getTimeReceivedFromServer() - request.getTimeEnqueued());
					// Write the request back to the client.
					this.resp.clear();
					this.resp.put((request.getResponseMsg()+"\r\n").getBytes());
					this.resp.flip();
					
					while(this.resp.hasRemaining()) {
						try {
							request.getSocketChannel().write(this.resp);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}catch (Exception e) {
				e.printStackTrace();
			}
				
			}
		}
	}
}
// END of the file
