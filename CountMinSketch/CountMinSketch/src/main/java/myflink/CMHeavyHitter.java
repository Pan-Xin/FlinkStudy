package myflink;

import myflink.Util.MurmurHash;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// solve the heavy hitter problem by using count-min sketch
public class CMHeavyHitter implements Serializable {

    private transient CountMinSketch countMinSketch;

    private HashMap<Object, Long> heavyHitter;

    private long cardinality;

    private double fraction;

    private double error;

    // the getters
    CountMinSketch getCountMinSketch(){
        return countMinSketch;
    }

    long getCardinality(){
        return cardinality;
    }

    double getFraction(){
        return fraction;
    }

    double getError(){
        return error;
    }

    // the constructors for count-min sketch heavy hitter
    public CMHeavyHitter(double fraction, double error, double confidence, int seed){
        this.countMinSketch = new CountMinSketch(error, confidence, seed);
        this.error = error;
        this.cardinality = 0;
        this.fraction = fraction;
        this.heavyHitter = new HashMap<Object, Long>();
    }

    public CMHeavyHitter(CountMinSketch countMinSketch, double fraction){
        this.countMinSketch = countMinSketch;
        this.error = countMinSketch.getEps();
        this.cardinality = 0;
        this.fraction = fraction;
        this.heavyHitter = new HashMap<Object, Long>();
    }

    public void add(Object obj) {
        cardinality += 1;
        // if the type of obj is Long
        if(obj instanceof  Long){
            countMinSketch.add((Long)obj, 1);
        }
        else{
            countMinSketch.add(MurmurHash.hash(obj), 1);
        }
        updateHeavyHitter(obj);
    }

    // update the heavy hitter
    private void updateHeavyHitter(Object obj) {
        // the obj in the heavy hitter should have a frequency
        // which is not less than the min value
        long minValue = (long)Math.ceil(cardinality * fraction);
        // get the estimate count for current obj
        long estimateCount = estimateCount(obj);
        if(estimateCount >= minValue)
            heavyHitter.put(obj, estimateCount);
        if(cardinality % (long)Math.ceil(1 / error) == 0)
            remove(minValue);
    }

    // remove those items from hashmap which frequency is less than min value
    private void remove(long minValue) {
        Iterator iterator = heavyHitter.entrySet().iterator();
        while(iterator.hasNext()){
            long temp = ((Map.Entry<Object, Long>)iterator.next()).getValue();
            if(temp < minValue)
                iterator.remove();
        }
    }

    // get the estimate count value from count-min sketch
    public long estimateCount(Object item){
        if(item instanceof Long)
            return countMinSketch.estimateCount((Long)item);
        else
            return countMinSketch.estimateCount(MurmurHash.hash(item));
    }

    public HashMap getHeavyHitter() {
        long minValue = (long)Math.ceil(cardinality * fraction);
        remove(minValue);
        return heavyHitter;
    }

    // the function used to merge two heavy hitter
    public void merge(CMHeavyHitter ch2) throws Exception {
        try {
            // check whether the fraction are the same
            if(this.fraction != ch2.fraction)
                throw new Exception("Two heavy hitters must have the same fraction");
            // merge the two count-min sketch
            this.countMinSketch = CountMinSketch.merge(this.countMinSketch, ch2.countMinSketch);
            // merge the two heavy hitter
            HashMap<Object, Long> res = new HashMap<Object, Long>();
            // copy current heavy hitter to the result
            for(Map.Entry<Object, Long> e : this.heavyHitter.entrySet()){
                // because this.countMinSketch has been updated
                // the estimate count here are the sum of this and ch2
                res.put(e.getKey(), estimateCount(e.getKey()));
            }
            // put the entry whose key are not in this into result
            for(Map.Entry<Object, Long> e : ch2.heavyHitter.entrySet()){
                if(!res.containsKey(e.getKey()))
                    res.put(e.getKey(), estimateCount(e.getKey()));
            }
            // update the result heavy hitter
            this.heavyHitter = res;
            // update the result cardinality
            this.cardinality += ch2.cardinality;
        } catch (ClassCastException e1){
            throw new Exception("Both heavy hitter should have the same class");
        } catch (Exception e2){
            throw new Exception("Fail to merge heavy hitters");
        }

    }

    public String toString() {
        String str = "";
        Map<Object, Long> heavyHitter = getHeavyHitter();
        for(Map.Entry<Object, Long> e : heavyHitter.entrySet()){
            str += e.getKey().toString() + "  frequency:" + e.getValue() + "\n";
        }
        return str;
    }

    private void writeObj(ObjectOutputStream out) throws IOException{
        out.defaultWriteObject();
        out.writeObject(CountMinSketch.serialize(countMinSketch));
    }

    private void readObj(ObjectInputStream in) throws  IOException, ClassNotFoundException{
        in.defaultReadObject();
        byte[] b = (byte[]) in.readObject();
        countMinSketch = CountMinSketch.deserialize(b);
    }

    public CMHeavyHitter clone(){
        CMHeavyHitter res = new CMHeavyHitter(Test.CMHHConfig.fraction, Test.CMHHConfig.error,
                Test.CMHHConfig.confidence, Test.CMHHConfig.seed);
        try {
            res.merge(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }
}
