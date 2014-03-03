package joiner.client;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import joiner.DataServerConnector;
import joiner.Prefix;
import joiner.twins.HashTwinFunction;
import joiner.twins.TwinFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class Client  {
	
	private final static Logger logger = LoggerFactory.getLogger(Client.class);
	
	private final ZContext context;
	private final Socket socket;
	private final Cipher cipher;
	
	private final List<?> markers;
	private final TwinFunction twin;
	
	private Set<String> receivedTwins;
	private Set<String> receivedMarkers;
	private List<String> matches;
	
	public Client(String key, List<?> markers, TwinFunction twin) throws Exception {
		this.cipher = createCipher(key);
		this.markers = markers;
		this.twin = twin;
		this.context = new ZContext();
		this.socket = context.createSocket(ZMQ.PAIR);
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
		matches = new LinkedList<String>();
		receivedMarkers = new HashSet<String>();
		receivedTwins = new HashSet<String>();
		
		// receive the messages and process them until completed
		while (true) {
			byte[] bytes = socket.recv();
			if (bytes.length == 0)
				break;
			processMessage(bytes);
		}
		
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
			matches.add(payload);
			break;

		case MARKER:
			logger.debug("marker: {}", payload);
			receivedMarkers.add(payload);
			break;

		case TWIN:
			logger.debug("twin: {}", payload);
			receivedTwins.add(payload);
			break;

		default:
			logger.error("UNEXPECTED RECEIVED DATA");
		}
	}
	
	private boolean validateResult() {
		// prevent short-circuit evaluation
		boolean valid = validateMarkers() & validateTwins();
		
		if (valid)
			logger.info("VALID RESULT: {}", matches);
		else
			logger.info("RESULT NOT VALID");
		
		return valid;
	}

	private boolean validateMarkers() {
		List<String> missingMarkers = new LinkedList<String>();
		
		for (Object marker: markers) {
			String markerString = marker.toString();
			if (receivedMarkers.contains(markerString))
				receivedMarkers.remove(markerString);
			else
				missingMarkers.add(markerString);
		}
		
		logger.info("{} matched markers", markers.size() - receivedMarkers.size(), markers.size());
		logger.info("{} missing markers: {}", missingMarkers.size(), missingMarkers);
		logger.info("{} unmatched markers: {}", receivedMarkers.size(), receivedMarkers);
		return (missingMarkers.isEmpty() && receivedMarkers.isEmpty());
	}
	
	private boolean validateTwins() {
		List<String> missingTwins = new LinkedList<String>();
		int matchedTwins = 0;
		
		for (String match: matches) {
			if (twin.neededFor(match)) {
				if (receivedTwins.contains(match)) {
					++matchedTwins;
					receivedTwins.remove(match);
				} else
					missingTwins.add(match);
			}
		}
		
		logger.info("{} matched twins", matchedTwins);
		logger.info("{} missing twins: {}", missingTwins.size(), missingTwins);
		logger.info("{} unmatched twins: {}", receivedTwins.size(), receivedTwins);
		return (missingTwins.isEmpty() && receivedTwins.isEmpty());
	}
	
	public static void main(String[] args) {
		// example: ThisIsASecretKey tcp://127.0.0.1:5555 tcp://127.0.0.1:3000 1 10000 tcp://127.0.0.1:3000 8000 20000 10 0 1 2 3 4 5 6 7 8 9
		
		if (args.length < 9) {
			logger.error("args: cipherKey csString sc1String table1 col1 sc2String table2 col2 oneTwinOutOf markers...");
			System.exit(1);
		}
		
		int index = 0;
		String cipherKey = args[index++];
		String csString = args[index++];
		
		DataServerConnector sc1 = new DataServerConnector(args[index++], args[index++], args[index++]);
		DataServerConnector sc2 = new DataServerConnector(args[index++], args[index++], args[index++]);
		
		int oneTwinOutOf = Integer.parseInt(args[index++]);
		TwinFunction twin = new HashTwinFunction(oneTwinOutOf);
		
		List<String> markers = new LinkedList<String>();
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
