package myflink;

import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;

public class CMtest {
    static final double fraction = 0.01;
    static final double error = 0.005;
    static final double confidence = 0.99;
    static final int seed = 7362181;
    static final Random r = new Random();
    static final int cardinality = 1000000;
    static final int maxScale = 100000;

    @Test
    public void testAccuracy() {

        long[] actualFreq = new long[maxScale];

        CMHeavyHitter cmTopK = new CMHeavyHitter(fraction,error,confidence,seed);

        for (int i = 0; i < cardinality; i++) {
            int value;
            if (r.nextDouble()<0.1){
                value = r.nextInt(10);
            }else{
                value = r.nextInt(maxScale);
            }
            cmTopK.add(value);
            actualFreq[value]++;
        }

        long frequency = (long)Math.ceil(cardinality* fraction);
        for (int i=0;i<actualFreq.length;i++){
            if (actualFreq[i]>frequency){
                assertTrue("Heavy Hitter not found :" + i +","+ actualFreq[i], cmTopK.getHeavyHitter().containsKey(i));
            }
        }

        Iterator it = cmTopK.getHeavyHitter().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry heavyHitter = (Map.Entry)it.next();
            Long estimateError = (Long)heavyHitter.getValue() - actualFreq[(Integer)heavyHitter.getKey()];
            assertTrue("Difference between real frequency and estimate is too large: " + estimateError,
                    estimateError < (error*cardinality));
        }
    }

    @Test
    public void merge() throws Exception {

        int numToMerge = 5;

        long[] actualFreq = new long[maxScale];
        CMHeavyHitter merged = new CMHeavyHitter(fraction,error,confidence,seed);
        long totalCardinality = 0;

        CMHeavyHitter[] sketches = new CMHeavyHitter[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            CountMinSketch cms = new CountMinSketch(error, confidence, seed);
            sketches[i] = new CMHeavyHitter(cms, fraction);
            for (int j = 0; j < cardinality; j++) {
                int val;
                if (r.nextDouble()<0.1){
                    val = r.nextInt(10);
                }else{
                    val = r.nextInt(maxScale);
                }
                sketches[i].add(val);
                actualFreq[val]++;
                totalCardinality++;
            }
            merged.merge(sketches[i]);
        }

        Map<Object,Long> mergedHeavyHitters = merged.getHeavyHitter();
        long frequency = (long)(totalCardinality*fraction);

        for (int i = 0; i < actualFreq.length; ++i) {
            if (actualFreq[i] >= frequency) {
                assertTrue("All items with freq. > s.n will be found. Item " + i + ". Real freq. " + actualFreq[i] + " Expected freq." + frequency, mergedHeavyHitters.containsKey(i));
            }
        }
    }
}
