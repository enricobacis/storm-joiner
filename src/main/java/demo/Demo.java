package demo;

import java.util.HashSet;
import java.util.Set;

import joiner.client.Client;
import joiner.commons.DataServerConnector;
import joiner.commons.twins.HashTwinFunction;
import joiner.commons.twins.TwinFunction;
import joiner.computational.ComputationalServer;
import joiner.server.DataServer;

public class Demo {

	public static void main(String[] args) throws Exception {

		// create the markers
		Set<String> markers = new HashSet<String>();
		for (int i = 0; i < 100; ++i)
			markers.add(Integer.toString(i));

		// create the twin function
		TwinFunction twin = new HashTwinFunction(10);

		// create the data server
		DataServer ds = new DataServer(3000, "ThisIsASecretKey", markers, twin);
		ds.start();

		ComputationalServer cs = new ComputationalServer(5555, 2);
		cs.start();

		// create the client and execute the query
		Client client = new Client("ThisIsASecretKey", markers, twin);
		client.connect("tcp://127.0.0.1:5555");

		DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1:3000", "1", "50");
		DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1:3000", "1", "50");

		client.join(sc1, sc2);
		client.destroy();
	}

}
