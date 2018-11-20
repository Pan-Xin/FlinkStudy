package myflink.Util;


import java.io.UnsupportedEncodingException;

// provide hash function for string
public class HashForString {

    public static int[] getHashBuckets(String item, int depth, int width){
        byte[] b;
        try{
            b = item.getBytes("UTF-16");
        } catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }
        int[] res = new int[depth];
        int hash1 = MurmurHash.hash(b, b.length, 0);
        int hash2 = MurmurHash.hash(b, b.length, hash1);
        for(int i=0; i<depth; i++){
            res[i] = Math.abs((hash1 + i * hash2) % width);
        }
        return res;
    }
}
