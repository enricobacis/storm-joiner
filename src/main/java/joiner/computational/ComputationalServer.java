package joiner.computational;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ComputationalServer extends Thread {
	
	private final ZContext context;
	private final Socket clientSocket;
	
	public ComputationalServer() {
		context = new ZContext();
		clientSocket = context.createSocket(ZMQ.PAIR);
	}
	
	@Override
	public void run() {
    	try {
    		
			clientSocket.bind("tcp://*:5555");
			System.out.println("Computational Server up");
		
			while (!Thread.currentThread().isInterrupted()) {
	            String request = new String(clientSocket.recv());
	            System.out.println(request);
	            
	            for (int i = 1; i < 10; ++i) {
	            	clientSocket.send(request + ' ' + i);
					sleep(200);
	            }
	            
	            clientSocket.send("");
	        }
		
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			clientSocket.close();
			context.destroy();
		}
	}
	
	public static void main(String[] args) {
		
		ComputationalServer cs = new ComputationalServer();
		cs.start();

	}

}
