package joiner.client;

import joiner.JoinData;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class Client  {
	
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
	
	public void queryJoin(JoinData ... datas) {
		for (JoinData data: datas) {
			socket.sendMore(data.getConnectionString() + " " + data.getColumn());
		}
		socket.send("");
		
		String message;
		while (true) {
			message = new String(socket.recv());
			
			if (message.isEmpty())
				break;
			
			System.out.println(message);
		}
	}
	
	public static void main(String[] args) {
		
		System.out.println("up");
		
		Client client = new Client();
		client.connect("tcp://127.0.0.1:5555");
		client.queryJoin(new JoinData("1", "A"), new JoinData("2", "B"));
		client.destroy();
		
		System.out.println("done");
	}
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
	}

}
