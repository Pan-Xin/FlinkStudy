package myflink;

//The interface of frequency contains some operators for the algorithm
public interface FrequencyInterface {

    // for long type elements
    void add(long item, long count);

    long estimateCount(long item);

    // for String type elements
    void add(String item, long count);

    long estimateCount(String item);

    long size();

}
