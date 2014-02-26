package joiner;


public class JoinData {
	
	private String connectionString;
	private String column;
	
	public JoinData(String connectionString, String column) {
		this.connectionString = connectionString;
		this.column = column;
	}
	
	public String getConnectionString() {
		return connectionString;
	}
	
	public String getColumn() {
		return column;
	}
	
}
