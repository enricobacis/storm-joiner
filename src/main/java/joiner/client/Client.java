package joiner.client;

import joiner.DataServerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class Client  {
	
	private final Logger logger = LoggerFactory.getLogger(Client.class);
	
	private ZContext context;
	private Socket socket;
	
	public Client() {
		context = new ZContext();
		socket = context.createSocket(ZMQ.PAIR);
	}
	
	public void connect(String connectionString) {
		socket.connect(connectionString);
	}
	
	public void destroy() {
		context.destroy();
	}
	
	public void queryJoin(DataServerConnector ... datas) {
		
		for (DataServerConnector data: datas) {
			socket.sendMore(data.toMsg());
		}
		socket.send("");
		
		String message;
		while (true) {
			message = new String(socket.recv());
			
			if (message.isEmpty())
				break;
			
			logger.info(message);
		}
	}
	
	public static void main(String[] args) {		
		Client client = new Client();
		client.connect("tcp://127.0.0.1:5555");
		
		DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1", 3000, "3", "4");
		DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1", 3000, "C", "D");
		
		client.queryJoin(sc1, sc2);
		System.out.println("done");
		client.destroy();
	}

}
