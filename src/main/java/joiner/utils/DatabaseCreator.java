package joiner.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;


public abstract class DatabaseCreator {
	
	protected static void create(Connection connection, String table, String column, int from, int to, int step, int repetitions) throws Exception {
		
		Statement stmt = connection.createStatement();
		stmt.execute(String.format("CREATE TABLE %s (%s int NOT NULL)", table, column));
		stmt.execute(String.format("CREATE INDEX main ON %s (%s)", table, column));
		
		PreparedStatement ps = connection.prepareStatement(String.format("INSERT INTO %s VALUES (?)", table));
		
		for (int i = from; i <= to; i += step) {
			ps.setInt(1, i);
			for (int j = 0; j < repetitions; ++j)
				ps.addBatch();
		}
		
		ps.executeBatch();
		
		connection.commit();
		connection.close();
	}

}
