package myflink;

import java.util.HashMap;

//the interface for heavy hitter problem
public interface HeavyHitter {

    void add(Object obj);

    HashMap getHeavyHitter();

    void merge(HeavyHitter heavyHitter) throws Exception;

    String toString();
}
