package joiner.commons.twins;


public abstract class TwinFunction {
	
	protected final int oneEvery;
	
	public TwinFunction(int oneEvery) {
		this.oneEvery = oneEvery;
	}
	
	public abstract boolean neededFor(String o);
	
	public double getPercent() {
		return 1.0 / oneEvery;
	}
	
}
