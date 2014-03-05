package demo;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joiner.client.Client;
import joiner.commons.DataServerConnector;
import joiner.commons.twins.HashTwinFunction;
import joiner.commons.twins.TwinFunction;
import joiner.computational.ComputationalServer;
import joiner.server.DataServer;

public class Test {
	
	private final static Logger logger = LoggerFactory.getLogger(Test.class);
	
	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;

	public static void execute(int megaBytesTableA, int megaBytesTableB, float matchPercent, float twinPercent, int numMarkers, int joinerThreads, boolean pipeline) throws Exception {
		
		logger.info("pipeline {}", pipeline ? "ACTIVE" : "INACTIVE");

		// create the markers
		Set<String> markers = new HashSet<String>();
		for (int i = 0; i < numMarkers; ++i)
			markers.add(Integer.toString(i));

		// create the twin function
		int oneTwinEvery = (int) Math.ceil(1 / twinPercent);
		logger.info("One twin every {}", oneTwinEvery);
		TwinFunction twin = new HashTwinFunction(oneTwinEvery);

		// create the data server
		DataServer ds = new DataServer(3000, "ThisIsASecretKey", markers, twin, pipeline);
		ds.last(4);
		ds.start();

		ComputationalServer cs = new ComputationalServer(5555, joinerThreads, pipeline);
		cs.last(1);
		cs.start();

		// create the client and execute the query
		Client client = new Client("ThisIsASecretKey", markers, twin);
		client.connect("tcp://127.0.0.1:5555");
		
		int fromA = 1; 
		int toA = MBtoData(megaBytesTableA);
		int fromB = (int) (toA * (1 - matchPercent)) + 1;
		int toB = fromB + MBtoData(megaBytesTableB) - 1;
		
		logger.info("Table A from {} to {}", fromA, toA);
		logger.info("Table B from {} to {}", fromB, toB);

		DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1:3000", Integer.toString(fromA), Integer.toString(toA));
		DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1:3000", Integer.toString(fromB), Integer.toString(toB));

		long initial = System.nanoTime();
		client.join(sc1, sc2);
		float elapsed = (System.nanoTime() - initial) / ((float) BILLION);
		
		logger.info("Elapsed time: {} s", elapsed);
		client.destroy();
	}
	
	private static int MBtoData(int megaBytes) {
		return megaBytes * (MILLION / 16);
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 7) {
			logger.error("args: pipeline megaBytesTableA megaBytesTableB matchPercent twinPercent markers joinerThreads");
			System.exit(1);
		}

		int index = 0;
		boolean pipeline = Boolean.parseBoolean(args[index++]); 
		int megaBytesTableA = Integer.parseInt(args[index++]);
		int megaBytesTableB = Integer.parseInt(args[index++]);
		float matchPercent = Float.parseFloat(args[index++]);
		float twinPercent = Float.parseFloat(args[index++]);
		int markers = Integer.parseInt(args[index++]);
		int joinerThreads = Integer.parseInt(args[index++]);

		Test.execute(megaBytesTableA, megaBytesTableB, matchPercent, twinPercent, markers, joinerThreads, pipeline);
	}

}
