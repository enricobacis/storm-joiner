package joiner.server;

import java.sql.Connection;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import joiner.DataServerRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.google.common.base.Function;

public class BaseDataServer extends Thread {
	
	private final Logger logger = LoggerFactory.getLogger(BaseDataServer.class);
	
	private final Cipher cipher;
	private final Connection database;
	
	private final int helloPort;
	private ZContext helloContext;
	private Socket helloSocket;
	
	private int lastPort;
	
	public BaseDataServer(Connection database, int helloPort, String AESkey, Function<Object, Boolean> needsTwin) throws Exception {
		this.database = database;
		this.helloPort = helloPort;
		this.lastPort = helloPort;
		
		cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		// TODO make it salted
		SecretKey secretKey = new SecretKeySpec(AESkey.getBytes(), "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
	}
	
	@Override
	public void run() {
		
		try {
			helloContext = new ZContext();
			helloSocket = helloContext.createSocket(ZMQ.REP);
			helloSocket.bind("tcp://*:" + helloPort);
			logger.info("DataServer UP");
			
			while (!Thread.currentThread().isInterrupted())
				receiveRequest();
			
		} catch ( Exception e ) {
			logger.error(e.getMessage());
		} finally {
			helloContext.destroy();
			logger.info("DataServer DOWN");
		}
	}
	
	private void receiveRequest() {
		DataServerRequest request = DataServerRequest.fromMsg(helloSocket.recv());
		logger.info("Received: " + request);
		DataWorker dw = new DataWorker(++lastPort, request, cipher);
		dw.start();
		helloSocket.send(Integer.toString(lastPort));
	}

}
