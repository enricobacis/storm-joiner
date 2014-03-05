package joiner.computationalstorm;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import joiner.commons.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinerBolt extends BaseRichBolt {
	
	@SuppressWarnings("unused")
	private final Logger logger = LoggerFactory.getLogger(JoinerBolt.class);
	
	private static final long serialVersionUID = 2505025320748351627L;
	
	// TODO generalize to n tables using an hashmap
	
	private OutputCollector collector;
	private Set<Bytes> keys;
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.keys = new HashSet<Bytes>(10000000);
	}

	@Override
	public void execute(Tuple tuple) {
		Bytes message = (Bytes) tuple.getValue(0);
		
		if (message.isEmpty())
			collector.emit(new Values(message));
		else if (keys.contains(message)) {
			keys.remove(message);
			collector.emit(new Values(message));
		} else
			keys.add(message);
		
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key"));
	}

}
