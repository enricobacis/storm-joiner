package joiner.server;

import java.util.LinkedList;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import joiner.DataServerRequest;
import joiner.twins.HashTwinFunction;
import joiner.twins.TwinFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class DataServer extends Thread {
	
	private final static Logger logger = LoggerFactory.getLogger(DataServer.class);
	
	private final int helloPort;
	private final List<?> markers;
	private final TwinFunction twin;
	
	private int lastUsedPort;
	private Socket helloSocket;
	private Cipher cipher;
	
	public DataServer(int helloPort, String key, List<?> markers, TwinFunction twin) throws Exception {
		this.helloPort = helloPort;
		this.lastUsedPort = helloPort;
		this.markers = markers;
		this.twin = twin;
		this.cipher = createCipher(key);
	}
	
	@Override
	public void run() {
		
		ZContext context = new ZContext();
		helloSocket = context.createSocket(ZMQ.REP);
		
		try {
			
			// create the hello socket
			helloSocket.bind("tcp://*:" + helloPort);
			logger.info("DataServer UP");
			
			// receive the incoming requests
			while (!Thread.currentThread().isInterrupted())
				receiveRequest();
			
		} catch ( Exception e ) {
			
			logger.error(e.getMessage());
			
		} finally {
			
			context.close();
			context.destroy();
			logger.info("DataServer DOWN");
			
		}
	}

	private Cipher createCipher(String key) throws Exception {
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKey secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		return cipher;
	}
	
	private void receiveRequest() {
		DataServerRequest request = DataServerRequest.fromMsg(helloSocket.recv());
		logger.info("Received: " + request);
		helloSocket.send(Integer.toString(++lastUsedPort));
		DataWorker dw = new DataWorker(lastUsedPort, request, cipher, markers, twin);
		dw.start();
	}
	
	public static void main(String[] args) {
		// example: 3000 ThisIsASecretKey 10 0 1 2 3 4 5 6 7 8 9
		
		if (args.length < 3) {
			logger.error("args: helloPort cipherKey oneTwinOutOf markers...");
			System.exit(1);
		}
		
		int index = 0;
		int helloPort = Integer.parseInt(args[index++]);
		String cipherKey = args[index++];
		
		int oneTwinOutOf = Integer.parseInt(args[index++]);
		TwinFunction twin = new HashTwinFunction(oneTwinOutOf);
		
		List<String> markers = new LinkedList<String>();
		for (int i = index; i < args.length; ++i)
			markers.add(args[i]);
		
		try {
			DataServer ds = new DataServer(helloPort, cipherKey, markers, twin);
			ds.start();
		} catch (Exception e) {
			logger.error("Server error");
			e.printStackTrace();
		}
	}

}
