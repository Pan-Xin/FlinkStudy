package myflink.LossyCountingForHeavyHitters;

import java.io.Serializable;
import java.util.Map;

// implement the lossy counting algorithm for sloving heavy hitters problem
public class LossyCounting implements Serializable {
    // here are the parameters for lossy counting data structure
    private double fraction;
    private double error;
    private long cardinality;
    private Map<Object, LCCounter> heavyHitters;

    // the constructors
    LossyCounting(double fraction, double error){
        this.fraction = fraction;
        this.error = error;
    }

    // add an item
    public void add(Object item){
        // if the heavy hitters has this item, then update the counter's value
        if(heavyHitters.containsKey(item))
            heavyHitters.get(item).add(1);
        // else new a new counter and add it into the heavy hitters
        else
            heavyHitters.put(item, new LCCounter(1, bucket));


        cardinality += 1;
    }



}
