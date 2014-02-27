package joiner.computational;

import java.util.Map;
import java.util.Observer;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ZmqSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 7219752650135883752L;
	
	private SpoutOutputCollector collector;
	private String socketString;
	private ZContext context;
	private Socket input;
	
	public ZmqSpout(String socketString) {
		this.socketString = socketString;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.context = new ZContext();
		this.input = this.context.createSocket(ZMQ.PULL);
		this.input.connect(socketString);
	}

	@Override
	public void nextTuple() {
		String message = new String(input.recv());
		
		if (message.isEmpty()) {
			// TODO SEND TO INPROC
			
			// NO-OP
			while (true)
				Utils.sleep(1000);
			
		} else
			collector.emit(new Values(message));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key"));
	}

}
