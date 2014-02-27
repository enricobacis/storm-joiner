package joiner.computational;

import java.util.ArrayList;
import java.util.List;

import joiner.DataServerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ComputationalServer extends Thread {

	private final Logger logger = LoggerFactory.getLogger(ComputationalServer.class);
	
	// TODO get from request
	private final int JoinerParallelism = 2;
	private final int numWorkers = 2;

	private final int incomingPort;
	private ZContext context;
	private Socket clientSocket;

	public ComputationalServer(int incomingPort) {
		this.incomingPort = incomingPort;
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
			
			ZmqBolt output = new ZmqBolt(); 
			JoinerTopology topology = new JoinerTopology(JoinerParallelism, numWorkers, output);

			for (DataServerConnector connector: receiveConnectors()) {
				int port = connector.handShake();
				logger.info("Assigned port: {} => {}", connector.getConnectionString(), port);
				topology.addSpout(connector.getConnectionString() + ':' + port);
			}
			
			topology.start();

		} catch (Exception e) {
			clientSocket.send("ERROR: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		ComputationalServer cs = new ComputationalServer(5555);
		cs.start();
	}

}
