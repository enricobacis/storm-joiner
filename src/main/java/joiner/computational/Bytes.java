package joiner.computational;

import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;

public class Bytes {
	
	private final byte[] bytes;
	private Integer hashcode = null;
	private String string = null;

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
    	if (hashcode == null)
    		hashcode = Arrays.hashCode(bytes); 
        return hashcode;
    }
    
    @Override
    public String toString() {
    	if (string == null)
    		string = Base64.encodeBase64String(bytes);
    	return string;
    }

}
