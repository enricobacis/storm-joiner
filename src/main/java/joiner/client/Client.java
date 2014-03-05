package joiner.client;

import java.util.HashSet;
import java.util.Observable;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import joiner.commons.DataServerConnector;
import joiner.commons.Prefix;
import joiner.commons.twins.HashTwinFunction;
import joiner.commons.twins.TwinFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class Client extends Observable {
	
	private final static Logger logger = LoggerFactory.getLogger(Client.class);
	
	private final ZContext context;
	private final Socket socket;
	private final Cipher cipher;
	
	private final Set<String> markers;
	private final TwinFunction twin;
	
	private Set<String> pendingTwins;
	private Set<String> pendingMarkers;
	private int receivedData;
	private int receivedTwins;
	
	public Client(String key, Set<String> markers, TwinFunction twin) throws Exception {
		this.cipher = createCipher(key);
		this.markers = markers;
		this.twin = twin;
		this.context = new ZContext();
		this.socket = context.createSocket(ZMQ.PAIR);
		this.socket.setHWM(100000000);
		this.socket.setLinger(-1);
	}
	
	private Cipher createCipher(String key) throws Exception {
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKey secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
		cipher.init(Cipher.DECRYPT_MODE, secretKey);
		return cipher;
	}
	
	public void connect(String connectionString) {
		socket.connect(connectionString);
	}
	
	public void destroy() {
		context.destroy();
	}
	
	public void join(DataServerConnector ... connectors) throws Exception {
		
		// send the connectors to the computational server
		sendDataConnectors(connectors);
		
		// initialize the data structures
		pendingMarkers = new HashSet<String>(markers);
		pendingTwins = new HashSet<String>();
		receivedData = 0;
		receivedTwins = 0;
		
		// receive the messages and process them until completed
		while (true) {
			byte[] bytes = socket.recv();
			
			if (bytes.length == 0)
				break;
			
			processMessage(bytes);
		}
		
		socket.send("ACK");
		validateResult();
	}
	
	private void sendDataConnectors(DataServerConnector[] connectors) {
		logger.info("starting join");
		for (DataServerConnector connector: connectors)
			socket.sendMore(connector.toMsg());
		socket.send("");
	}
	
	private void processMessage(byte[] bytes) throws Exception {
		// decipher the message
		String message = new String(cipher.doFinal(bytes), "UTF-8");
		
		// split the message in its prefix and its payload
		Prefix prefix = Prefix.of(message.charAt(0));
		String payload = message.substring(1);
		
		// process the prefix and the payload
		process(prefix, payload);
	}
	
	private void process(Prefix prefix, String payload) {
		switch (prefix) {
		
		case DATA:
			logger.debug("match: {}", payload);
			++receivedData;
			
			if (twin.neededFor(payload))
				xorSet(pendingTwins, payload);
			
			setChanged();
			notifyObservers(payload);
			break;

		case MARKER:
			logger.debug("marker: {}", payload);
			xorSet(pendingMarkers, payload);
			break;

		case TWIN:
			logger.debug("twin: {}", payload);
			++receivedTwins;
			xorSet(pendingTwins, payload);
			break;

		default:
			logger.error("UNEXPECTED RECEIVED DATA");
		}
	}
	
	private boolean validateResult() {
		// prevent short-circuit evaluation
		boolean valid = validateMarkers() & validateTwins();
		
		if (valid)
			logger.info("RESULT VALID. #RECEIVED: {}", receivedData);
		else
			logger.info("RESULT NOT VALID. #RECEIVED: {}", receivedData);
		
		return valid;
	}

	private boolean validateMarkers() {
		logger.info("{} matched markers", markers.size() - pendingMarkers.size());
		logger.info("{} unmatched markers: {}", pendingMarkers.size(), pendingMarkers);
		return pendingMarkers.isEmpty();
	}
	
	private boolean validateTwins() {
		logger.info("{} matched twins", receivedTwins - pendingTwins.size());
		logger.info("{} missing twins: {}", pendingTwins.size(), pendingTwins);
		return pendingTwins.isEmpty();
	}
	
	private <T> void xorSet(Set<T> set, T elem) {
		if (set.contains(elem))
			set.remove(elem);
		else
			set.add(elem);
	}
	
	public static void main(String[] args) {
		// example: ThisIsASecretKey tcp://127.0.0.1:5555 tcp://127.0.0.1:3000 1 10000 tcp://127.0.0.1:3000 8000 20000 10 0 1 2 3 4 5 6 7 8 9
		
		if (args.length < 9) {
			logger.error("args: cipherKey csString sc1String table1 col1 sc2String table2 col2 oneTwinEvery markers...");
			System.exit(1);
		}
		
		int index = 0;
		String cipherKey = args[index++];
		String csString = args[index++];
		
		DataServerConnector sc1 = new DataServerConnector(args[index++], args[index++], args[index++]);
		DataServerConnector sc2 = new DataServerConnector(args[index++], args[index++], args[index++]);
		
		int oneTwinEvery = Integer.parseInt(args[index++]);
		TwinFunction twin = new HashTwinFunction(oneTwinEvery);
		
		Set<String> markers = new HashSet<String>();
		for (int i = index; i < args.length; ++i)
			markers.add(args[i]);
		
		try {
			Client client = new Client(cipherKey, markers, twin);
			client.connect(csString);
			client.join(sc1, sc2);
			client.destroy();
			
		} catch (Exception e) {
			logger.error("Server error");
			e.printStackTrace();
		}
	}

}
