package joiner;

import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

public class DataServerConnector {
	
	private final String connectionString;
	private final int helloPort;
	private DataServerRequest request;
	private ZContext context;
	
	public DataServerConnector(String connectionString, int helloPort, String table, String column) {
		this.connectionString = connectionString;
		this.helloPort = helloPort;
		this.request = new DataServerRequest(table, column);
	}
	
	public int handShake() throws Exception {
		
		context = new ZContext();
		Socket server = context.createSocket(ZMQ.REQ);
		server.connect(connectionString + ':' + helloPort);
		
		server.send(request.toMsg());
		String reply = new String(server.recv());
		context.destroy();
		
		if (reply.startsWith("ERROR"))
			throw new Exception(reply);
		
		return Integer.parseInt(reply);
	}
	
	public String getConnectionString() {
		return connectionString;
	}
	
	public int getHelloPort() {
		return helloPort;
	}
	
	public String toMsg() {
		return connectionString + '\t' + helloPort + '\t' + request.toMsg();
	}
	
	public static DataServerConnector fromMsg(String msg) {
		String[] parts = msg.split("\t");
		return new DataServerConnector(parts[0], Integer.parseInt(parts[1]), parts[2], parts[3]);
	}

	@Override
	public String toString() {
		return "DataServerConnector [connectionString=" + connectionString
				+ ", helloPort=" + helloPort + ", request=" + request + "]";
	}
	
}
