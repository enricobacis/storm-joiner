package joiner.server;

import java.nio.ByteBuffer;

import javax.crypto.Cipher;

import joiner.DataServerRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class DataWorker extends Thread {

	private final Logger logger = LoggerFactory.getLogger(DataWorker.class);
	
	private final int outputPort;
	private final Cipher cipher;
    private final DataServerRequest request;
	
	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher) {
		this.outputPort = outputPort;
		this.request = request;
		this.cipher = cipher;
	}
	
	@Override
	public void run() {
		
		ZContext context = new ZContext();
		Socket socket = context.createSocket(ZMQ.PUSH);
		
		try {
			socket.setHWM(1000000);
			socket.bind("tcp://*:" + outputPort);
			
			logger.info("Start pushing data to port {}", outputPort);
			
			for (int i = 0; i < 10; ++i) {
				socket.send(cipher.doFinal(ByteBuffer.allocate(4).putInt(i).array()));
			}
			
			// Signal the end of the connection
			socket.send("");
			
			logger.info("All data pushed to port {}", outputPort);
			
			// Avoid closing the socket before elements are put in the queue
			Thread.sleep(1000);
			
		} catch ( Exception e ) {
			
			logger.error(e.getMessage());
			socket.send("SERVER ERROR: " + e.getMessage());
			
		} finally {
			
			// the LINGER option is already set to -1 (default) so the context.close()
			// method waits for all the messages to get pulled
			context.close();
			context.destroy();
			
		}
		
	}
	
}
