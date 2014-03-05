package joiner.commons;

public class DataServerRequest {
	public final String table;
	public final String column;

	public DataServerRequest(String table, String column) {
		this.table = table;
		this.column = column;
	}

	public String getTable() {
		return table;
	}

	public String getColumn() {
		return column;
	}

	public String toMsg() {
		return table + '\t' + column;
	}
	
	public static DataServerRequest fromMsg(String msg) {
		String[] parts = msg.split("\t");
		return new DataServerRequest(parts[0], parts[1]);
	}
	
	public static DataServerRequest fromMsg(byte[] msg) {
		return fromMsg(new String(msg));
	}

	@Override
	public String toString() {
		return "DataServerRequest [table=" + table + ", column=" + column + "]";
	}
	
}