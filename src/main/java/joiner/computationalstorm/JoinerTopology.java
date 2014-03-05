package joiner.computationalstorm;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class JoinerTopology extends Thread {
	
	private final Logger logger = LoggerFactory.getLogger(JoinerTopology.class);

	private final TopologyBuilder builder;
	private final String topologyName;
	private final String replySocketString;
	private final String killerSocketString;
	private final int joinerParallelism;
	private final int numWorkers;
	private final List<String> spouts;
	private final LocalCluster cluster;
	
	public JoinerTopology(String topologyName, int joinerParallelism, int numWorkers) {
		this.topologyName = topologyName;
		this.joinerParallelism = joinerParallelism;
		this.numWorkers = numWorkers;
		
		replySocketString = "ipc://" + topologyName;
		killerSocketString = replySocketString + "-killer";
		
		spouts = new ArrayList<String>();
		builder = new TopologyBuilder();
		cluster = new LocalCluster();
	}
	
	public void addSpout(String socketString, int parallelism_hint) {
		builder.setSpout(socketString, new ZmqSpout(socketString), parallelism_hint);
		spouts.add(socketString);
	}
	
	@Override
	public void run() {
		
    	// Start a listener for completed spouts
    	TopologyKiller killer = new TopologyKiller();
    	killer.start();
		
    	// create the joiner bolt
		BoltDeclarer declarer = builder.setBolt("join", new JoinerBolt(), joinerParallelism);
		
		// connect all the spouts to the joiner bolt using fieldGrouping
		for (String spout: spouts)
			declarer = declarer.fieldsGrouping(spout, new Fields("key"));
		
		// create the output bolt with globalGrouping
		builder.setBolt("output", new ZmqBolt(spouts.size(), replySocketString, killerSocketString), 1)
			.globalGrouping("join");
		
		Config conf = new Config();
	    conf.setNumWorkers(numWorkers);
	    conf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
	    conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100);
	    conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 20);
	    
	    try {
	    	logger.info("Starting topology");
	        cluster.submitTopology(topologyName, conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
			kill();
		}
	}
	
	private void kill() {
		logger.info("shutting down");
		cluster.killTopology(topologyName);
	    cluster.shutdown();
	}

	private class TopologyKiller extends Thread {
		
		private ZContext context;

		@Override
		public void run() {
			context = new ZContext();
			Socket killerSocket = context.createSocket(ZMQ.PAIR);
			killerSocket.bind(killerSocketString);
			
			// wait for the job to be completed
			killerSocket.recv();
			kill();
			
			context.destroy();
		}
	}

}
