package joiner.computationalstorm;

import java.util.ArrayList;
import java.util.List;

import joiner.commons.DataServerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ComputationalServer extends Thread {
	
	// TODO restructure class as ROUTER/DEALER with Workers

	private static final Logger logger = LoggerFactory.getLogger(ComputationalServer.class);
	private static int topologyCounter = 0;
	
	private final int numWorkers; // number of JVM on the cluster (normally to be set to one per node)
	private final int joinerParallelism; // number of executors (thread) of joiner bolts (spread to the workers)
	private final int spoutParallelism; // number of spout executors

	private final int incomingPort;
	private ZContext context;
	private Socket clientSocket;

	public ComputationalServer(int incomingPort, int numWorkers, int joinerParallelism, int spoutParallelism) {
		this.incomingPort = incomingPort;
		this.numWorkers = numWorkers;
		this.joinerParallelism = joinerParallelism;
		this.spoutParallelism = spoutParallelism;
	}

	@Override
	public void run() {

		context = new ZContext();
		clientSocket = context.createSocket(ZMQ.PAIR);
		
		try {
			
			clientSocket.bind("tcp://*:" + incomingPort);
			logger.info("ComputationalServer UP");

			while (!Thread.currentThread().isInterrupted())
				receiveRequest();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			context.destroy();
			logger.info("ComputationalServer DOWN");
		}

	}

	private List<DataServerConnector> receiveConnectors() {
		String msg;
		List<DataServerConnector> connectors = new ArrayList<DataServerConnector>();

		while (true) {
			msg = new String(clientSocket.recv());

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
			
			// TODO create a connection directly to the client (ROUTER/DEALER)
			String topologyName = "topology-" + (++topologyCounter);
			JoinerTopology topology = new JoinerTopology(topologyName, joinerParallelism, numWorkers);

			for (DataServerConnector connector: receiveConnectors()) {
				int port = connector.handShake();
				logger.info("Assigned port: {} => {}", connector.getConnectionString(), port);
				topology.addSpout(connector.getConnectionString().replaceFirst(":\\d+", ":" + port), spoutParallelism);
			}
			
			topology.start();
			
			// TODO to be removed when we have a connection directly to the client
			Socket topologySocket = context.createSocket(ZMQ.PAIR);
			topologySocket.bind("ipc://" + topologyName);
			while (true) {
				byte[] message = topologySocket.recv();
				clientSocket.send(message);
				if (message.length == 0)
					break;
			}

		} catch (Exception e) {
			clientSocket.send("ERROR: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		// example: 5555 1 2 1
		
		if (args.length != 4) {
			logger.error("args: incomingPort numWorkers joinerParallelism spoutParallelism");
			System.exit(1);
		}
		
		int index = 0;
		int incomingPort = Integer.parseInt(args[index++]);
		int numWorkers = Integer.parseInt(args[index++]);
		int joinerParallelism = Integer.parseInt(args[index++]);
		int spoutParallelism = Integer.parseInt(args[index++]);
		
		ComputationalServer cs = new ComputationalServer(incomingPort, numWorkers, joinerParallelism, spoutParallelism);
		cs.start();
	}

}
