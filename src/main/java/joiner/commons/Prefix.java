package joiner.commons;


public enum Prefix {
	
	DATA('0'),
	MARKER('1'),
	TWIN('2');
	
	private final char prefix;
	
	private Prefix(char prefix) {
		this.prefix = prefix;
	}
	
	public char getPrefix() {
		return prefix;
	}
	
	public static Prefix of(char prefix) {
		for (Prefix p: Prefix.values())
			if (p.getPrefix() == prefix)
				return p;
		return null;
	}

}
