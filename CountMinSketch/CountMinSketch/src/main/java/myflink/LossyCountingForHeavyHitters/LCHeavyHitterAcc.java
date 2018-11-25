package myflink.LossyCountingForHeavyHitters;

import org.apache.flink.api.common.accumulators.Accumulator;

public class LCHeavyHitterAcc implements Accumulator<Object, LossyCounting> {

    private LossyCounting local;

    private double pFraction = LCHeavyHitterConfig.pFraction;
    private double pError = LCHeavyHitterConfig.pError;

    // the constructor
    LCHeavyHitterAcc(){
        local = new LossyCounting(pFraction, pError);
    }

    @Override
    public void add(Object value) {
        local.add(value);
    }

    @Override
    public LossyCounting getLocalValue() {
        return local;
    }

    @Override
    public void resetLocal() {
        local = new LossyCounting(pFraction, pError);
    }

    @Override
    public void merge(Accumulator<Object, LossyCounting> other) {
        try {
            local.merge(other.getLocalValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Accumulator<Object, LossyCounting> clone() {
        LCHeavyHitterAcc res = new LCHeavyHitterAcc();
        try {
            res.local = local.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return res;
    }
}
