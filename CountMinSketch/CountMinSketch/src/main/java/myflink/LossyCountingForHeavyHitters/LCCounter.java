package myflink.LossyCountingForHeavyHitters;

import java.io.Serializable;

// the data structure of counters used in Lossy Counting algorithm
public class LCCounter implements Serializable {
    // here are the attributes of counters
    public long lowerBound;

    // the constructors
    LCCounter(){}

    LCCounter(long lowerBound, long frequencyError){
        this.lowerBound = lowerBound;
    }

    // when adding, update the lower bound
    public void add(long count){
        lowerBound += count;
    }

}
