package joiner.commons;


import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

public class DataServerConnector {
	
	private final String connectionString;
	private DataServerRequest request;
	private ZContext context;
	
	private int port;
	private int recordsHint;
	
	public DataServerConnector(String connectionString, String table, String column) {
		this.connectionString = connectionString;
		this.request = new DataServerRequest(table, column);
	}
	
	public String getConnectionString() {
		return connectionString;
	}
	
	public int getPort() {
		return port;
	}
	
	public int getRecordsHint() {
		return recordsHint;
	}
	
	public void handShake() throws Exception {
		
		context = new ZContext();
		Socket server = context.createSocket(ZMQ.REQ);
		server.connect(connectionString);
		
		server.send(request.toMsg());
		String reply = new String(server.recv());
		context.destroy();
		
		if (reply.startsWith("ERROR"))
			throw new Exception(reply);
		
		String[] parts = reply.split(" ");
		port = Integer.parseInt(parts[0]);
		recordsHint = Integer.parseInt(parts[1]);
	}
	
	public void done() {
		context = new ZContext();
		Socket server = context.createSocket(ZMQ.REQ);
		server.connect(connectionString);
		
		server.send("DONE " + port);
		server.recv();       // ACK
		context.destroy();
	}
	
	public String toMsg() {
		return connectionString + '\t' + request.toMsg();
	}
	
	public static DataServerConnector fromMsg(String msg) {
		String[] parts = msg.split("\t");
		return new DataServerConnector(parts[0], parts[1], parts[2]);
	}

	@Override
	public String toString() {
		return "DataServerConnector [connectionString=" + connectionString
				+ ", request=" + request + "]";
	}
	
}
