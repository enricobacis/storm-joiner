package joiner.computational;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinerBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 2505025320748351627L;
	
	// TODO generalize to n tables using an hashmap
	
	private OutputCollector collector;
	private Set<String> keys;
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.keys = new HashSet<String>(1000);
	}

	@Override
	public void execute(Tuple tuple) {
		String key = tuple.getString(0);
		
		if (keys.contains(key)) {
			keys.remove(key);
			collector.emit(new Values(key));
		} else
			keys.add(key);
		
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key"));
	}

}
