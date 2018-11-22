package myflink;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.io.Serializable;

// used to track estimated values for count distinct and heavy hitters
public class CMHeavyHitterAcc implements Accumulator<Object, Serializable>, Serializable {

    private CMHeavyHitter local;

    // parameters for count-min sketch which are defined by CMHeavyHitterConfig class
    private double pFraction = CMHeavyHitterConfig.fraction;
    private double pError = CMHeavyHitterConfig.error;
    private double pConfidence = CMHeavyHitterConfig.confidence;
    private int pSeed = CMHeavyHitterConfig.seed;

    // the constructor
    public CMHeavyHitterAcc(){
        local = new CMHeavyHitter(pFraction, pError, pConfidence, pSeed);
    }

    @Override
    public void add(Object value) {
        local.add(value);
    }

    @Override
    public CMHeavyHitter getLocalValue() {
        return local;
    }

    @Override
    public void resetLocal() {
        local = new CMHeavyHitter(pFraction, pError, pConfidence, pSeed);
    }

    @Override
    public void merge(Accumulator<Object, Serializable> other) {
        try {
            local.merge((CMHeavyHitter) other.getLocalValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Accumulator<Object, Serializable> clone() {
        CMHeavyHitterAcc clone = new CMHeavyHitterAcc();
        clone.local = this.local.clone();
        return clone;
    }
}
