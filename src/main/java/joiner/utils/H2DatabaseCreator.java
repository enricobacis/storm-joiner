package joiner.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class H2DatabaseCreator {

	public static void create(String jdbcString, String table, String column, int from, int to, int step) throws Exception {
		create(jdbcString, table, column, from, to, step, 1);
	}

	public static void create(String jdbcString, String table, String column, int from, int to, int step, int repetitions) throws Exception {
		Class.forName("org.h2.Driver");
		Connection connection = DriverManager.getConnection(jdbcString);
		DatabaseCreator.create(connection, table, column, from, to, step, repetitions);
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

		create("jdbc:h2:~/Desktop/test", "ta", "co", 1, 100, 3, 2);
		
	}
	
}
