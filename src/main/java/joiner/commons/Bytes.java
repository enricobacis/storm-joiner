package joiner.commons;

import java.util.Arrays;

public class Bytes {
	
	private final byte[] bytes;

    public Bytes(byte[] bytes) {
        if (bytes == null)
            throw new NullPointerException();
        this.bytes = bytes;
    }
    
    public byte[] getBytes() {
    	return bytes;
    }
    
    public boolean isEmpty() {
    	return bytes.length == 0;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Bytes))
            return false;
        return Arrays.equals(bytes, ((Bytes) other).bytes);
    }

    @Override
    public int hashCode() {
    	return Arrays.hashCode(bytes); 
    }
    
    @Override
    public String toString() {
    	return new String(bytes);
    }

}
