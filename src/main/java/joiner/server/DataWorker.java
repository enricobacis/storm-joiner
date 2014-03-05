package joiner.server;

import java.util.Set;

import javax.crypto.Cipher;

import joiner.commons.DataServerRequest;
import joiner.commons.Prefix;
import joiner.commons.twins.TwinFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class DataWorker extends Thread {

	private final Logger logger = LoggerFactory.getLogger(DataWorker.class);

	private final int outputPort;
	private final Cipher cipher;
	private final Set<String> markers;
	private final TwinFunction twin;
	private final int moreFlag;
	private Socket socket;

	private ZContext context;
	private boolean done = false;
	private int from, to;
	
	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin) {
		this(outputPort, request, cipher, markers, twin, true);
	}

	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin, boolean pipeline) {
		this.outputPort = outputPort;
		this.cipher = cipher;
		this.markers = markers;
		this.twin = twin;
		this.moreFlag = pipeline ? 0 : ZMQ.SNDMORE;

		parseRequest(request);
	}

	private void parseRequest(DataServerRequest request) {
		// This is a fake DataWorker, it generates the data
		from = Integer.parseInt(request.getTable());
		to = Integer.parseInt(request.getColumn());
	}

	public int getRecordsHint() {
		return (int) (markers.size() +
				(to - from + 1) * (1 + twin.getPercent()));
	}

	@Override
	public void run() {

		context = new ZContext();
		socket = context.createSocket(ZMQ.PUSH);

		try {

			// Open the output socket
			socket.bind("tcp://*:" + outputPort);
			logger.info("Start pushing data to port {}", outputPort);

			// Send all the markers (without their twins) [TODO shuffle them with the data]
			for (String marker: markers)
				encryptAndSend(marker, Prefix.MARKER, false);

			// Send the data (with the twins)
			for (int i = from; i <= to; ++i)
				encryptAndSend(Integer.toString(i), Prefix.DATA, true);

			// Signal the end of the connection
			socket.send("");
			logger.info("All data pushed to port {}", outputPort);

			while(!done)
				Thread.sleep(100);

		} catch ( Exception e ) {

			logger.error(e.getMessage());
			socket.send("SERVER ERROR: " + e.getMessage());

		} finally {
			context.destroy();
		}

	}

	private void encryptAndSend(String data, Prefix prefix, boolean addTwin) throws Exception {
		
		// send the message
		socket.send(cipher.doFinal((prefix.getPrefix() + data).getBytes("UTF-8")), moreFlag);

		// send the twin if requested and needed
		if (addTwin && twin.neededFor(data))
			socket.send(cipher.doFinal((Prefix.TWIN.getPrefix() + data).getBytes("UTF-8")), moreFlag);
	}

	public void done() {
		done = true;
	}

	@Override
	protected void finalize() throws Throwable {
		context.destroy();
		super.finalize();
	}

}
