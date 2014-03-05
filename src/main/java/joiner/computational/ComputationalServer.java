package joiner.computational;

import java.util.ArrayList;
import java.util.List;

import joiner.commons.DataServerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ComputationalServer extends Thread {

	private static final Logger logger = LoggerFactory.getLogger(ComputationalServer.class);
	
	private final int incomingPort;
	private final int joinerThreads;
	private final int moreFlag;
	
	private ZContext context;
	private Socket clientSocket;

	private String backendString;
	private Socket backendSocket;
	
	private int last = -1;
	
	public ComputationalServer(int incomingPort, int joinerThreads) {
		this(incomingPort, joinerThreads, true);
	}

	public ComputationalServer(int incomingPort, int joinerThreads, boolean pipeline) {
		this.incomingPort = incomingPort;
		this.joinerThreads = joinerThreads;
		this.backendString = "ipc://computational";
		this.moreFlag = pipeline ? 0 : ZMQ.SNDMORE;
	}
	
	public void last(int last) {
		this.last = last;
	}

	@Override
	public void run() {

		context = new ZContext();
		
		backendSocket = context.createSocket(ZMQ.PULL);
		backendSocket.bind(backendString);
		
		clientSocket = context.createSocket(ZMQ.PAIR);
		clientSocket.setHWM(1000000000);
		clientSocket.setLinger(-1);
		
		try {
			
			clientSocket.bind("tcp://*:" + incomingPort);
			logger.info("ComputationalServer UP");

			while (last != 0) {
				receiveRequest();
				if (last > 0) --last;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			context.destroy();
			logger.info("ComputationalServer DOWN");
		}

	}

	private List<DataServerConnector> receiveConnectors() {
		List<DataServerConnector> connectors = new ArrayList<DataServerConnector>();
		logger.info("Waiting for request");

		while (true) {
			String msg = new String(clientSocket.recv());

			if (!msg.isEmpty())
				connectors.add(DataServerConnector.fromMsg(msg));

			if (!clientSocket.hasReceiveMore())
				break;
		}

		logger.info("Received: {}", connectors);
		return connectors;
	}

	private void receiveRequest() {

		try {
			
			JoinerTopology topology = new JoinerTopology(backendString, joinerThreads);
			List<DataServerConnector> connectors = receiveConnectors();

			for (DataServerConnector connector: connectors) {
				connector.handShake();
				int port = connector.getPort();
				int recordsHint = connector.getRecordsHint();
				
				logger.info("Assigned port: {} => {}", connector.getConnectionString(), port);
				topology.addSpout(connector.getConnectionString().replaceFirst(":\\d+", ":" + port), recordsHint);
			}
			
			topology.start();
			
			int completedJoiners = 0;
			while (true) {
				byte[] message = backendSocket.recv();
				if (message.length == 0) {
					++completedJoiners;
					logger.info("{}/{} joiner completed", completedJoiners, joinerThreads);
					if (completedJoiners == joinerThreads)
						break;
				} else
					clientSocket.send(message, moreFlag);
			}
			
			topology.clean();
			for (DataServerConnector connector: connectors)
				connector.done();
			Thread.sleep(200);
			
			clientSocket.send("");
			clientSocket.recv();  // ACK
			
			logger.info("join completed");

		} catch (Exception e) {
			e.printStackTrace();
			clientSocket.send("ERROR: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		// example: 5555 2
		
		if (args.length != 3) {
			logger.error("args: pipeline incomingPort joinerThreads");
			System.exit(1);
		}
		
		int index = 0;
		boolean pipeline = Boolean.parseBoolean(args[index++]);
		int incomingPort = Integer.parseInt(args[index++]);
		int joinerThreads = Integer.parseInt(args[index++]);
		
		ComputationalServer cs = new ComputationalServer(incomingPort, joinerThreads, pipeline);
		cs.start();
	}

}
