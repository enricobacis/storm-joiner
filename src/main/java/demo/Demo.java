package demo;

import java.util.LinkedList;
import java.util.List;

import joiner.DataServerConnector;
import joiner.client.Client;
import joiner.computational.ComputationalServer;
import joiner.server.DataServer;
import joiner.twins.HashTwinFunction;
import joiner.twins.TwinFunction;

public class Demo {
	
	public static void main(String[] args) throws Exception {
		
		// create themarkers
		List<Integer> markers = new LinkedList<Integer>();
		for (int i = 0; i < 100; ++i)
			markers.add(i);

		// create the twin function
		TwinFunction twin = new HashTwinFunction(10);

		// create the data server
		DataServer ds = new DataServer(3000, "ThisIsASecretKey", markers, twin);
		ds.start();

		// create the computational server (1 worker (machine), 2 executors (threads))
		ComputationalServer cs = new ComputationalServer(5555, 1, 2, 1);
		cs.start();

		// create the client and execute the query
		Client client = new Client("ThisIsASecretKey", markers, twin);
		client.connect("tcp://127.0.0.1:5555");

		DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1:3000", "1", "10000");
		DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1:3000", "8000", "25000");

		client.join(sc1, sc2);
		client.destroy();
	}

}
