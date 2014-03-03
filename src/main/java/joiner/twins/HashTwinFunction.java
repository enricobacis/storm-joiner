package joiner.twins;

public class HashTwinFunction extends TwinFunction {

	public HashTwinFunction(int oneOutOf) {
		super(oneOutOf);
	}

	@Override
	public boolean neededFor(String o) {
		return (o.hashCode() % oneOutOf) == 0;
	}
	
}