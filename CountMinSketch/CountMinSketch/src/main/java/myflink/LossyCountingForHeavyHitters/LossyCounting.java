package myflink.LossyCountingForHeavyHitters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
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
        this.cardinality = 0;
        this.heavyHitters = new HashMap<Object, LCCounter>();
    }

    // add an item
    public void add(Object item){
        //update the cardinality
        cardinality += 1;
        // if the heavy hitters has this item, then update the counter's value
        if(heavyHitters.containsKey(item))
            heavyHitters.get(item).add(1);
        // else new a new counter and add it into the heavy hitters
        else
            heavyHitters.put(item, new LCCounter(1, 0));
        // if current window is full then update the window and the heavy hitters
        if(cardinality % (long) Math.ceil(1 / error) == 0)
            // update heavy hitters
            updateHeavyHitters();
    }

    // need to update the heavy hitters when turn to a new window
    private void updateHeavyHitters() {
        // each counter should minus 1 and if the counter is equal to 0 than drop it
        Iterator iterator = heavyHitters.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<Object, LCCounter> entry = (Map.Entry<Object, LCCounter>) iterator.next();
            entry.getValue().add(-1);
            if(entry.getValue().lowerBound <= 0)
                iterator.remove();
        }

//        for(Map.Entry<Object, LCCounter> entry : heavyHitters.entrySet()){
//            Object item = entry.getKey();
//            LCCounter counter = entry.getValue();
//            counter.add(-1);
//            if(counter.lowerBound <= 0)
//                heavyHitters.remove(item); // drop
//            else heavyHitters.put(item, counter); // update
//        }
    }

    // merge two loosy counting
    public void merge(LossyCounting lc2) throws Exception{
        try{
            if(lc2 == null) return;
            if(lc2.fraction != fraction)
                throw new Exception("Lossy Counting Merge Error: Fraction should be equal");
            cardinality += lc2.cardinality;
            Map<Object, LCCounter> hh2 = lc2.heavyHitters;
            for(Map.Entry<Object, LCCounter> entry : hh2.entrySet()){
                Object item = entry.getKey();
                LCCounter counter2 = entry.getValue();
                LCCounter counter1 = heavyHitters.get(item);
                if(counter1 == null) heavyHitters.put(item, counter2);
                else counter1.add(counter2.lowerBound);
                if(cardinality % (long) Math.ceil(1 / error) == 0)
                    updateHeavyHitters();
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    // return the heavy hitters
    public HashMap<Object, Long> getHeavyHitters(){
        HashMap<Object, Long> res = new HashMap<>();
        // define the threshold
        long threshold = (long) Math.ceil((fraction - error) * cardinality);
        for(Map.Entry<Object, LCCounter> entry : heavyHitters.entrySet()){
            Object item = entry.getKey();
            LCCounter counter = entry.getValue();
            // if the lowerBound is not less than the threshold
            // than put it into the result
            if(counter.lowerBound >= threshold)
                res.put(item, counter.lowerBound);
        }
        return res;
    }

    @Override
    public String toString() {
        String str = "";
        // get the heavy hitters
        Map<Object, Long> res = getHeavyHitters();
        for(Map.Entry<Object, Long> entry : res.entrySet())
            str += entry.getKey() + "  frequency: " + entry.getValue() + "\n";
        return str;
    }

    @Override
    protected LossyCounting clone() throws CloneNotSupportedException {
        LossyCounting res = new LossyCounting(fraction, error);
        res.cardinality = cardinality;
        Map<Object, LCCounter> map = new HashMap<>();
        for(Map.Entry<Object, LCCounter> entry : heavyHitters.entrySet())
            map.put(entry.getKey(), entry.getValue());
        res.heavyHitters = map;
        return res;
    }
}
