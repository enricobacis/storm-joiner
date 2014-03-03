package joiner.twins;


public abstract class TwinFunction {
	
	protected final int oneOutOf;
	
	public TwinFunction(int oneOutOf) {
		this.oneOutOf = oneOutOf;
	}
	
	public abstract boolean neededFor(String o);
	
}
