package myflink;

import org.apache.flink.api.common.accumulators.Accumulator;

public class CMHeavyHitterAcc implements Accumulator<Object, CMHeavyHitter> {

    private CMHeavyHitter local;

    public CMHeavyHitterAcc(){
        local = new CMHeavyHitter(Test.CMHHConfig.fraction, Test.CMHHConfig.error,
                Test.CMHHConfig.confidence, Test.CMHHConfig.seed);
    }

    @Override
    public void add(Object value) {
        local.process(value);
    }

    @Override
    public CMHeavyHitter getLocalValue() {
        return local;
    }

    @Override
    public void resetLocal() {
        local = new CMHeavyHitter(Test.CMHHConfig.fraction, Test.CMHHConfig.error,
                Test.CMHHConfig.confidence, Test.CMHHConfig.seed);
    }

    @Override
    public void merge(Accumulator<Object, CMHeavyHitter> other) {
        local.merge(other.getLocalValue());
    }

    @Override
    public Accumulator<Object, CMHeavyHitter> clone() {
        CMHeavyHitterAcc clone = new CMHeavyHitterAcc();
        clone.local = this.local.clone();
        return clone;
    }
}
