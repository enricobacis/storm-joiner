package joiner.commons.twins;

public class HashTwinFunction extends TwinFunction {

	public HashTwinFunction(int oneEvery) {
		super(oneEvery);
	}

	@Override
	public boolean neededFor(String o) {
		return (o.hashCode() % oneEvery) == 0;
	}
	
}