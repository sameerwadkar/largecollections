package utils;

import java.util.Arrays;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

public class KeyUtils {
    public static boolean debug=false;
    public static byte[] getPrefixedKey(String prefix,byte[] key){        
        byte[] lenPrefixBytes = Ints.toByteArray(prefix.getBytes().length);
        byte[] prefixBytes = prefix.getBytes();
        byte[] bytes = Bytes.concat(lenPrefixBytes,prefixBytes,key);
        return bytes;
    }
    public static byte[] getKey(byte[] fullKey){    
        int lengthOfPrefix =Ints.fromByteArray(Arrays.copyOfRange(fullKey,0,4));
        byte[] prefixBytes = Arrays.copyOfRange(fullKey,4,4+lengthOfPrefix);
        byte[] keyBytes = Arrays.copyOfRange(fullKey,4+lengthOfPrefix,fullKey.length);
        if(debug){
            System.out.println("Debug");
            System.out.println("\tLength of Prefix="+lengthOfPrefix);
            System.out.println("\tPrefix="+new String(prefixBytes));
            System.out.println("\tKey="+new String(keyBytes));
        }
        return keyBytes;
    }
    
    public static byte[][] getPrefixAndKey(byte[] fullKey){    
        int lengthOfPrefix =Ints.fromByteArray(Arrays.copyOfRange(fullKey,0,4));
        byte[] prefixBytes = Arrays.copyOfRange(fullKey,4,4+lengthOfPrefix);
        byte[] keyBytes = Arrays.copyOfRange(fullKey,4+lengthOfPrefix,fullKey.length);
        byte[][] result = new byte[2][];
        result[0]=prefixBytes;
        result[1]=keyBytes;
        return result;
    }
    
    public static void main(String[] args){
        KeyUtils.debug = true;
        byte[] x = KeyUtils.getPrefixedKey("pre", "key".getBytes());
        byte[][] y = KeyUtils.getPrefixAndKey(x);
        System.err.println(new String(y[0]));
        System.err.println(new String(y[1]));
    }
}
