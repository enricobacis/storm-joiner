package joiner.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;


public class DatabaseCreator {
	
	public static void create(String db, String table, String column, int from, int to, int step) throws Exception {
		create(db, table, column, from, to, step, 1);
	}
	
	public static void create(String db, String table, String column, int from, int to, int step, int repetitions) throws Exception {
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:" + db);
		
		Statement stmt = conn.createStatement();
		stmt.execute(String.format("CREATE TABLE %s (%s int NOT NULL)", table, column));
		stmt.execute(String.format("CREATE INDEX main ON %s (%s)", table, column));
		
		PreparedStatement ps = conn.prepareStatement(String.format("INSERT INTO %s VALUES (?)", table));
		
		for (int i = from; i <= to; i += step) {
			ps.setInt(1, i);
			for (int j = 0; j < repetitions; ++j)
				ps.addBatch();
		}
		
		ps.executeBatch();
		
		conn.commit();
		conn.close();
	}
	
	public static void main(String[] args) throws Exception {
		
//		if (args.length != 7)
//			System.err.println("Args: db table column from, to, step, repetitions");
//		else
//			create(args[0] // db
//				  ,args[1] // table
//				  ,args[2] // column
//				  ,Integer.parseInt(args[3])  // from
//				  ,Integer.parseInt(args[4])  // to
//				  ,Integer.parseInt(args[5])  // step
//				  ,Integer.parseInt(args[6])  // repetitions
//				  );
		
		create("~/Desktop/test", "ta", "co", 1, 100, 3, 2);
		
	}

}
