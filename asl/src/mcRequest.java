package asl;

/*
  This class represents a memcache request which holds the socket channel from where request came 
  and all timestamps used for instrumentation purpose. 
 
*/

import java.nio.channels.SocketChannel;
public class mcRequest {
	private byte[] rawRequest;			// Complete raw request 
	private SocketChannel socketChannel;// Socket channel from where the request came
	private String responseMsg;			// used to keep the response message only in case of a SET operation
	private long timeReceived;           // Time when the request was received by the middleware.
	private long timeResponseSent;       // Time when the response is sent back to the client.
	private long timeEnqueued;           // Time when the request was enqueued.
	private long timeDequeued;           // Time when the request was dequeued.
	private long timeParseStart;		 // Time when request goes for parsing
	private long timeParseEnd;			 // Time when parsing of request end
	private long timeSentToServer;       // Time when request is sent to the server
	private long timeReceivedFromServer; // Time when the response is received from the server.
	
	/* A long set of functions that is used to read and set the values of different parameters */
	
	public long getTimeParseEnd() {
		return timeParseEnd;
	}
	public void setTimeParseEnd(long timeParseEnd) {
		this.timeParseEnd = timeParseEnd;
	}
	public long getTimeParseStart() {
		return timeParseStart;
	}
	public void setTimeParseStart(long timeParseStart) {
		this.timeParseStart = timeParseStart;
	}
	public String getResponseMsg() {
		return responseMsg;
	}
	public void setResponseMsg(String responseMsg) {
		this.responseMsg = responseMsg;
	}
	public long getTimeReceived() {
		return timeReceived;
	}
	public void setTimeReceived(long timeReceived) {
		this.timeReceived = timeReceived;
	}
	public long getTimeResponseSent() {
		return timeResponseSent;
	}
	public void setTimeResponseSent(long timeResponseSent) {
		this.timeResponseSent = timeResponseSent;
	}
	public long getTimeEnqueued() {
		return timeEnqueued;
	}
	public void setTimeEnqueued(long timeEnqueued) {
		this.timeEnqueued = timeEnqueued;
	}
	public long getTimeDequeued() {
		return timeDequeued;
	}
	public void setTimeDequeued(long timeDequeued) {
		this.timeDequeued = timeDequeued;
	}
	public long getTimeSentToServer() {
		return timeSentToServer;
	}
	public void setTimeSentToServer(long timeSentToServer) {
		this.timeSentToServer = timeSentToServer;
	}
	public long getTimeReceivedFromServer() {
		return timeReceivedFromServer;
	}
	public void setTimeReceivedFromServer(long timeReceivedFromServer) {
		this.timeReceivedFromServer = timeReceivedFromServer;
	}
	public byte[] getRawRequest() {
		return rawRequest;
	}
	public void setRawRequest(byte[] rawRequest) {
		this.rawRequest = rawRequest;
	}	
	public SocketChannel getSocketChannel() {
		return socketChannel;
	}
	public void setSocketChannel(SocketChannel socketChannel) {
		this.socketChannel = socketChannel;
	}
}
