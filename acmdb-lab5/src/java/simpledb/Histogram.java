package simpledb;

public class Histogram<T> {

    public void addValue(T s) {}

    public double estimateSelectivity(Predicate.Op op, T s) { return 0; }

}
