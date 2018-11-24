package myflink.CountMinForHeavyHitters;

import java.io.Serializable;
import java.util.Random;

// the class defines the parameters for count-min heavy hitter
public class CMHeavyHitterConfig implements Serializable {
    static final double fraction = 0.01;
    static final double error = 0.005;
    static final double confidence = 0.99;
    static final int seed = 7362181;
}
