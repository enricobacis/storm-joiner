package joiner;

import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

public class DataServerConnector {
	
	private final String connectionString;
	private DataServerRequest request;
	
	public DataServerConnector(String connectionString, String table, String column) {
		this.connectionString = connectionString;
		this.request = new DataServerRequest(table, column);
	}
	
	public String getConnectionString() {
		return connectionString;
	}
	
	public int handShake() throws Exception {
		
		ZContext context = new ZContext();
		Socket server = context.createSocket(ZMQ.REQ);
		server.connect(connectionString);
		
		server.send(request.toMsg());
		String reply = new String(server.recv());
		context.close();
		context.destroy();
		
		if (reply.startsWith("ERROR"))
			throw new Exception(reply);
		
		return Integer.parseInt(reply);
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
