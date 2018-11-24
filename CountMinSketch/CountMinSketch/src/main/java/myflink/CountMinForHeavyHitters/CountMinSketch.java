package myflink.CountMinForHeavyHitters;

import myflink.Util.HashForString;
import org.apache.flink.util.Preconditions;

import java.io.*;
import java.util.Arrays;
import java.util.Random;

// the data structure for count-min sketch
// implements the FrequencyInterface (which contains basic operators for computing frequency)
public class CountMinSketch implements Serializable {

    // the big prime used in count-min hash functions
    public static final long COUNT_MIN_PRIME = (1L << 31) - 1;

    private static final long serialVersionUID = 1L;

    // here is the basic parameters for count-min sketch
    int depth; // the depth
    int width; // the width
    long[][] table; // the counters table
    long[] hashArr; // the hash function array
    long size;
    double eps; // the error
    double confidence; // the confidence

    // the getters
    public int getDepth(){
        return depth;
    }

    public int getWidth(){
        return width;
    }

    public long getSize(){
        return size;
    }

    public double getEps(){
        return eps;
    }

    public double getConfidence(){
        return confidence;
    }

    // the constructors
    CountMinSketch(){
    }

    // use depth, width, seed to build the count-min sketch
    public CountMinSketch(int depth, int width, int seed){
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
    }

    // use the eps, confidence, seed to build the count-min sketch
    public CountMinSketch(double eps, double confidence, int seed){
        this.eps = eps;
        this.confidence = confidence;
        // use the eps and confidence to compute the width and depth
        this.width = (int) Math.ceil(2 / eps);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        initTables(depth, width, seed);
    }

    // use the depth, width, size, hashArr, table to build the count-min sketch
    public CountMinSketch(int depth, int width, long size, long[] hashArr, long[][] table){
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.hashArr = hashArr;
        this.table = table;
        // check whether the size is smaller than 0
        Preconditions.checkState(size >= 0, "The size can not be smaller than 0");
        this.size = size;
    }


    // initialize tables function
    // depth and width the size of count-min sketch
    // seed is used to generate random number
    private void initTables(int depth, int width, int seed){
        this.table = new long[depth][width];
        this.hashArr = new long[depth];
        // generate the hash functions
        // hash function form：(a * item + b) mod p mod width
        // a, b are random numbers range from 1 to p - 1
        // we can set b = 0 because it does not change the independence
        Random random = new Random(seed);
        for(int i=0; i<depth; i++){ // p is equal to MAX_INT
            hashArr[i] = random.nextInt(Integer.MAX_VALUE);
        }
    }

    // compute the hash value in row i for item
    int hash(long item, int i){
        long hash = hashArr[i] * item;
        hash += hash >> 32;
        hash &= COUNT_MIN_PRIME;
        return ((int) hash) % width;
    }


    public void add(long item, long count) {
        // check whether it is an negative increment
        if(count < 0){
            throw new IllegalArgumentException("Negative increment");
        }
        // for each row, add count to the position hash(item)
        for(int i=0; i<depth; i++){
            table[i][hash(item, i)] += count;
        }
        checkSizeAfterAdd(String.valueOf(item), count);
    }

    public long estimateCount(long item) {
        // set a max number to initialize
        long res = Long.MAX_VALUE;
        // search for the min result in the table
        for(int i=0; i<depth; i++){
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    public void add(String item, long count) {
        // check whether it is an negative increment
        if(count < 0){
            throw new IllegalArgumentException("Negative increment");
        }
        int[] buckets = HashForString.getHashBuckets(item, depth, width);
        for(int i=0; i<depth; i++){
            table[i][buckets[i]] += count;
        }
        checkSizeAfterAdd(item, count);
    }

    public long estimateCount(String item) {
        // set a max number to initialize
        long res = Long.MAX_VALUE;
        // search for the min result in the table
        int[] buckets = HashForString.getHashBuckets(item, depth, width);
        for(int i=0; i<depth; i++){
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    public long size() {
        return size;
    }

    @Override
    public String toString() {
        return "Count-Min Sketch ("+
                "depth:"+depth+", "+
                "width:"+width+", "+
                "size:"+size+", "+
                "eps:"+eps+", "+
                "confiden:"+confidence+")";
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) return true;
        if(obj == null || getClass() != obj.getClass()){
            return false;
        }

        final CountMinSketch c = (CountMinSketch) obj;

        if(depth != c.depth) return false;
        if(width != c.width) return false;
        if(size != c.size) return false;
        if(Double.compare(c.eps, eps) != 0) return false;
        if(Double.compare(c.confidence, confidence) != 0) return false;
        if(!Arrays.deepEquals(table, c.table)) return false;
        return Arrays.equals(hashArr, c.hashArr);

    }

    @Override
    public int hashCode() {
        int res;
        long temp;
        res = depth;
        res = 31 * res + width;
        res = 31 * res + Arrays.deepHashCode(table);
        res = 31 * res + Arrays.hashCode(hashArr);
        res = 31 * res + (int)(size ^ (size >>> 32));
        temp = Double.doubleToLongBits(eps);
        res = 31 * res + (int)(temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(confidence);
        res = 31 * res + (int)(temp ^ (temp >>> 32));
        return res;
    }

    private void checkSizeAfterAdd(String item, long count) {
        long prevSize = size;
        size += count;
        if(size < prevSize){
            throw new IllegalStateException("Overflow after adding. " +
                    "Previous size:" + prevSize + " , current size:" + size) ;
        }
    }

    // merge several count-min sketches to build one count-min sketch
    public static CountMinSketch merge(CountMinSketch... sketches) throws Exception {
        CountMinSketch res = null;
        if(sketches != null && sketches.length > 0){
            int resDepth = sketches[0].depth;
            int resWidth = sketches[0].width;
            long[] resHashArr = Arrays.copyOf(sketches[0].hashArr, sketches[0].hashArr.length);
            long[][] resTable = new long[resDepth][resWidth];
            long resSize = 0;
            for(CountMinSketch sketch: sketches){
                if(sketch.depth != resDepth){
                    throw new Exception("Merge Error: depth is different");
                }
                if(sketch.width != resWidth){
                    throw new Exception("Merge Error: width is different");
                }
                if(!Arrays.equals(sketch.hashArr, resHashArr)){
                    throw new Exception("Merge Error: seed is different");
                }
                // it is ok to merge
                for(int i=0; i<resTable.length; i++){
                    for(int j=0; j<resTable[i].length; j++)
                        resTable[i][j] += sketch.table[i][j];
                }
                long prevSize = resSize;
                resSize += sketch.size;
                if(resSize < prevSize){
                    throw new IllegalStateException("Overflow after merging");
                }
            }
            res = new CountMinSketch(resDepth, resWidth, resSize, resHashArr, resTable);
        }
        return res;
    }

    public static byte[] serialize(CountMinSketch sketch) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(bos);
        try {
            s.writeLong(sketch.size);
            s.writeInt(sketch.depth);
            s.writeInt(sketch.width);
            for (int i = 0; i < sketch.depth; ++i) {
                s.writeLong(sketch.hashArr[i]);
                for (int j = 0; j < sketch.width; ++j) {
                    s.writeLong(sketch.table[i][j]);
                }
            }
            return bos.toByteArray();
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

    public static CountMinSketch deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream s = new DataInputStream(bis);
        try {
            CountMinSketch sketch = new CountMinSketch();
            sketch.size = s.readLong();
            sketch.depth = s.readInt();
            sketch.width = s.readInt();
            sketch.eps = 2.0 / sketch.width;
            sketch.confidence = 1 - 1 / Math.pow(2, sketch.depth);
            sketch.hashArr = new long[sketch.depth];
            sketch.table = new long[sketch.depth][sketch.width];
            for (int i = 0; i < sketch.depth; ++i) {
                sketch.hashArr[i] = s.readLong();
                for (int j = 0; j < sketch.width; ++j) {
                    sketch.table[i][j] = s.readLong();
                }
            }
            return sketch;
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

}
