package demo;

import joiner.DataServerConnector;
import joiner.client.Client;
import joiner.computational.ComputationalServer;
import joiner.server.BaseDataServer;

public class Demo {
	
	public static void main(String[] args) {
		try {
			
			BaseDataServer ds = new BaseDataServer(null, 3000, "0123456789ABCDEF", null);
			ds.start();
			
			ComputationalServer cs = new ComputationalServer(5555);
			cs.start();
			
			Client client = new Client();
			client.connect("tcp://127.0.0.1:5555");
			
			DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1", 3000, "table1", "joinCol1");
			DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1", 3000, "table2", "joinCol2");
			
			client.queryJoin(sc1, sc2);
			client.destroy();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
