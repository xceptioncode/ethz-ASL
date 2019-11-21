package asl;

import java.util.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.concurrent.LinkedBlockingQueue;

/*


*/

public class netThread implements Runnable
{
	private String MwListenAddr;
	private int MwListenPort;
	private List<String> McAddrs;
	private int numThreadsPTP;		
	private int writeToCount;	
	private boolean sharded;
	private Selector selector;
	private ByteBuffer buffer = ByteBuffer.allocate(1024);
	private ServerSocketChannel channel;
	private LinkedBlockingQueue<mcRequest> requestsQueue;  // Main requests queue which is responsible for holding the request
	private int totalserver;
	private static int queueLength = 0;
		
	public netThread(String MwListenAddr, int MwListenPort, List<String> McAddrs,
			int numThreadsPTP, boolean sharded) throws IOException {
			
				this.MwListenAddr = MwListenAddr;
				this.MwListenPort = MwListenPort;
				this.McAddrs = McAddrs;
				this.numThreadsPTP = numThreadsPTP;
				this.sharded = sharded;
				this.totalserver = McAddrs.size();
				
				requestsQueue = new LinkedBlockingQueue<mcRequest>();
				

				this.selector = Selector.open();
				this.channel = ServerSocketChannel.open();
				this.channel.socket().bind(new InetSocketAddress(this.MwListenAddr, this.MwListenPort));
				this.channel.configureBlocking(false);
				this.channel.register(this.selector, SelectionKey.OP_ACCEPT);  // Register the channel to perform operations with java NIO selector 

				for (int j=0; j<numThreadsPTP; j++) {
						new Thread(new workerThread(this.McAddrs, this.requestsQueue, this.sharded)).start();
					}
			}
				
	
	private void accept(SelectionKey key) throws IOException {   // Accept the connection from the client

		ServerSocketChannel netThreadChannel = (ServerSocketChannel) key.channel();
		SocketChannel channel = netThreadChannel.accept();
		Socket socket = channel.socket();
		channel.configureBlocking(false);
		channel.register(this.selector, SelectionKey.OP_READ);
	}
	
	private void readChannel(SelectionKey key) throws IOException, InterruptedException {  // Read the request from channel, start at the notice of read event
			SocketChannel channel = (SocketChannel) key.channel();
			this.buffer.clear();
			try {
				while (true) {
					int bytesRead = channel.read(this.buffer); // read the buffer

					if (bytesRead == 0) {  // Nothing was read
						break;
					}

					if (bytesRead == -1) {  // Channel got closed unexpectedly
						channel.close();
						return;
					}

				}
			}
			catch (IOException e) { 
				channel.close(); 
				key.cancel();
			}
		
			this.buffer.flip();	
			byte[] temp = this.buffer.array();
			mcRequest request = new mcRequest();  // Create a request representation
			request.setRawRequest(Arrays.copyOfRange(temp, 0, this.buffer.remaining()));  // Save the raw request 
			request.setSocketChannel(channel);
			request.setTimeEnqueued(System.nanoTime());  // The time when request was enqueued into the requests queue
			this.requestsQueue.put(request);					
				//System.out.println("Request is in Requests Queue");		
	}

	@Override
	public void run() {
		
	try {
		while(true) { // While loop for processing requests
		 this.selector.select(); 
		 Iterator iter = selector.selectedKeys().iterator();  
		 while (iter.hasNext()) {  
			 SelectionKey key = (SelectionKey) iter.next();  
			 iter.remove(); 
			 if(key.isAcceptable()) this.accept(key);
			 if(key.isReadable()) {
				try {
					this.readChannel(key);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
				}
			}
		}
	}
	catch (Exception e) {
			e.printStackTrace();
		}
	}
}