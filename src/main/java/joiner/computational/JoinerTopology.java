package joiner.computational;

import java.util.ArrayList;
import java.util.List;

import org.zeromq.ZMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class JoinerTopology extends Thread {
	
	private static int topologyCounter = 0;
	
	private final Logger logger = LoggerFactory.getLogger(JoinerTopology.class);

	private final TopologyBuilder builder;
	private final String topologyName;
	private final String killerSocketString;
	private final int joinerParallelism;
	private final int numWorkers;
	private final ZmqBolt output;
	private final List<String> spouts;

	private LocalCluster cluster;
	
	public JoinerTopology(int joinerParallelism, int numWorkers, ZmqBolt output) {
		this.builder = new TopologyBuilder();
		this.topologyName = "topology-" + (++topologyCounter);
		this.killerSocketString = "inproc://killer-" + this.topologyName;
		this.joinerParallelism = joinerParallelism;
		this.numWorkers = numWorkers;
		this.output = output;
		this.spouts = new ArrayList<String>();
		this.cluster = new LocalCluster();
	}
	
	public void addSpout(String socketString) {
		builder.setSpout(socketString, new ZmqSpout(socketString, killerSocketString), 1);
		spouts.add(socketString);
	}
	
	@Override
	public void run() {
		
		logger.info("Starting topology");
		
		BoltDeclarer declarer = builder.setBolt("join", new JoinerBolt(), joinerParallelism);
		for (String spout: spouts)
			declarer = declarer.fieldsGrouping(spout, new Fields("key"));
		builder.setBolt("output", output, 1).globalGrouping("join");
		
		Config conf = new Config();
	    conf.setNumWorkers(numWorkers);
        //  conf.setDebug(false);
	    
	    try {
	    	// Start a listener for completed spouts
	    	TopologyKiller killer = new TopologyKiller();
	    	killer.start();
	    	
	        cluster.submitTopology(topologyName, conf, builder.createTopology());
	        
	        // TODO remove after test
	        Utils.sleep(30000);
	        kill();
	        
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private void kill() {
		logger.info("shutting down");
		cluster.killTopology(topologyName);
	    cluster.shutdown();
	}

	private class TopologyKiller extends Thread {
		
		private final ZContext context;
		private final Socket killerSocket;
		private int completedSpouts = 0;
		
		public TopologyKiller() {
			this.context = new ZContext();
			this.killerSocket = context.createSocket(ZMQ.REQ);
		}
		
		@Override
		public void run() {
			killerSocket.bind(killerSocketString);
			
			while (completedSpouts != spouts.size()) {
				killerSocket.recv();
				++completedSpouts;
				logger.info("=== {}/{} spouts completed ===", completedSpouts, spouts.size());
			}
			
			context.destroy();
			kill();
		}
	}

}
