package joiner.server;

import java.util.List;

import javax.crypto.Cipher;

import joiner.DataServerRequest;
import joiner.Prefix;
import joiner.twins.TwinFunction;

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
    private final List<?> markers;
    private final TwinFunction twin;
    private Socket socket;
	
	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, List<?> markers, TwinFunction twin) {
		this.outputPort = outputPort;
		this.request = request;
		this.cipher = cipher;
		this.markers = markers;
		this.twin = twin;
	}
	
	@Override
	public void run() {
		
		ZContext context = new ZContext();
		socket = context.createSocket(ZMQ.PUSH);
		
		try {
			// Tell the socket to not drop the packets
			socket.setHWM(1000000);
			
			// Open the output socket
			socket.bind("tcp://*:" + outputPort);
			logger.info("Start pushing data to port {}", outputPort);
			
			// This is a fake DataWorker, it generates the data
			int from = Integer.parseInt(request.getTable());
			int to = Integer.parseInt(request.getColumn());
			
			// Send all the markers (without their twins) [TODO shuffle them with the data]
			for (Object marker: markers)
				encryptAndSend(marker.toString(), Prefix.MARKER, false);
			
			// Send the data (with the twins)
			for (int i = from; i <= to; ++i)
				encryptAndSend(Integer.toString(i), Prefix.DATA, true);
			
			// Signal the end of the connection
			socket.send("");
			logger.info("All data pushed to port {}", outputPort);
			
			// Avoid closing the socket before elements are put in the queue
			Thread.sleep(60000);
			
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
	
	private void encryptAndSend(String data, Prefix prefix, boolean addTwin) throws Exception {
		// send the message
		socket.send(cipher.doFinal((prefix.getPrefix() + data).getBytes("UTF-8")));
		
		// send the twin if requested and needed
		if (addTwin && twin.neededFor(data))
			socket.send(cipher.doFinal((Prefix.TWIN.getPrefix() + data).getBytes("UTF-8")));
	}
	
}
