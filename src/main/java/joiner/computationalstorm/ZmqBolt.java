package joiner.computationalstorm;

import java.util.Map;

import joiner.commons.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ZmqBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = -7874832150635980999L;
	private final Logger logger = LoggerFactory.getLogger(ZmqBolt.class);
	
	private final String replySocketString;
	private final String killerSocketString;
	
	private final int connectedSpouts;
	private int completedSpouts;
	
	private OutputCollector collector;
	private ZContext context;
	private Socket killer;
	private Socket reply;
	
	public ZmqBolt(int connectedSpouts, String replySocketString, String killerSocketString) {
		this.connectedSpouts = connectedSpouts;
		this.completedSpouts = 0;
		this.replySocketString = replySocketString;
		this.killerSocketString = killerSocketString;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.context = new ZContext();
		
		killer = this.context.createSocket(ZMQ.PAIR); 
		killer.connect(killerSocketString);
		reply = this.context.createSocket(ZMQ.PAIR);
		reply.connect(replySocketString);
	}

	@Override
	public void execute(Tuple tuple) {
		Bytes message = (Bytes) tuple.getValue(0);
		
		if (message.isEmpty()) {
			++completedSpouts;
			logger.info("=== {}/{} spouts completed ===", completedSpouts, connectedSpouts);
			if (completedSpouts == connectedSpouts) {
				killer.send("COMPLETED");
				reply.send("");
			}
		} else
			reply.send(message.getBytes());
		
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key"));
	}

}
