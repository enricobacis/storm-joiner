package joiner.computational;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class JoinerTopology extends Thread {
	
	private static final Logger logger = LoggerFactory.getLogger(JoinerTopology.class);
	
	private final ZContext context;
	private final Socket input;
	private final String outputString;
	
	private Socket[] joiners;
	private ComputationalWorker[] workers;
	private int numSpouts = 0;
	private int totalRecordsHint = 0;
	
	public JoinerTopology(String outputString, int numJoiners) {
		this.outputString = outputString;
		joiners = new Socket[numJoiners];
		workers = new ComputationalWorker[numJoiners];
		context = new ZContext();
		input = context.createSocket(ZMQ.PULL);
	}
	
	public void addSpout(String socketString, int recordsHint) {
		input.connect(socketString);
		totalRecordsHint += recordsHint;
		++numSpouts;
	}
	
	@Override
	public void run() {
		
		logger.info("join started");
		
		try {
			
			createJoiners();
			processInputs();
			Thread.sleep(100);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			context.destroy();
		}
	}
	
	private void createJoiners() {
		
		int numJoiners = joiners.length;
		int entriesHint = (int) (totalRecordsHint / numJoiners / 0.7);
		
		for (int i = 0; i < numJoiners; ++i) {
			String socketString = "ipc://joiner-" + i;
			
			ComputationalWorker worker = new ComputationalWorker(socketString, outputString, entriesHint);
			worker.start();
			workers[i] = worker;
			
			Socket socket = context.createSocket(ZMQ.PUSH);
			socket.connect(socketString);
			joiners[i] = socket;
		}
	}
	
	private void processInputs() {
		
		int numJoiners = joiners.length;
		int completedSpouts = 0;
		
		while (true) {
			byte[] message = input.recv();
			
			if (message.length == 0) {
				++completedSpouts;
				logger.info("{}/{} spouts completed", completedSpouts, numSpouts);
				if (completedSpouts == numSpouts)
					break;
			} else {
				int joiner = (Arrays.hashCode(message) & Integer.MAX_VALUE) % numJoiners;
				joiners[joiner].send(message);
			}
		}
		
		// send termination to joiners
		for (Socket joiner: joiners)
			joiner.send("");
		
		logger.info("all data sent to joiners");
	}
	
	public void clean() {		
		for (ComputationalWorker worker: workers)
			worker.done();
		
		workers = null;
		joiners = null;
	}
	
	@Override
	protected void finalize() throws Throwable {
		clean();
		super.finalize();
	}
	
}
