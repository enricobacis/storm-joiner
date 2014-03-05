package joiner.computationalstorm;

import java.util.Map;

import joiner.commons.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ZmqSpout extends BaseRichSpout {
	
	private final Logger logger = LoggerFactory.getLogger(ZmqSpout.class);
	private static final long serialVersionUID = 7219752650135883752L;

	private final String socketString;
	
	private SpoutOutputCollector collector;
	private ZContext context;
	private Socket input;
	
	public ZmqSpout(String socketString) {
		this.socketString = socketString;
		logger.debug(socketString);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.context = new ZContext();
		
		input = this.context.createSocket(ZMQ.PULL);
		input.connect(socketString);
	}
	
	@Override
	public void nextTuple() {
		Bytes message = new Bytes(input.recv());
		collector.emit(new Values(message));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key"));
	}

}
