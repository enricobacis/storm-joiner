package joiner.computational;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class JoinerTopology extends Thread {
	
	private final Logger logger = LoggerFactory.getLogger(JoinerTopology.class);

	private final TopologyBuilder builder;
	private final int joinerParallelism;
	private final int numWorkers;
	private final ZmqBolt output;
	private final List<String> spouts;

	private LocalCluster cluster;
	private int completedSpouts = 0;
	
	public JoinerTopology(int joinerParallelism, int numWorkers, ZmqBolt output) {
		this.builder = new TopologyBuilder();
		this.joinerParallelism = joinerParallelism;
		this.numWorkers = numWorkers;
		this.output = output;
		this.spouts = new ArrayList<String>();
		this.cluster = new LocalCluster();
	}
	
	public void addSpout(String socketString) {
		builder.setSpout(socketString, new ZmqSpout(socketString), 1);
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
	    
	    // TODO set to false after test
	    conf.setDebug(false);
	    
	    try {
	    	// TODO better name
	        cluster.submitTopology("joiner", conf, builder.createTopology());
	        
	        // TODO remove after test
	        Utils.sleep(30000);
	        kill();
	        
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private void kill() {
		logger.info("shutting down");
		cluster.killTopology("joiner");
	    cluster.shutdown();
	}
//
//	@Override
//	public void update(Observable o, Object arg) {
//		++completedSpouts;
//		logger.info("=== {}/{} spouts completed ===", completedSpouts, spouts.size());
//		if (completedSpouts == spouts.size())
//			kill();
//	}

}
