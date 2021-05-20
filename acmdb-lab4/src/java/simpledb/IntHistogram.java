package simpledb;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram extends Histogram<Integer> {

    private int buckets, min, max, sm;
    private int[] hist;
    private double binWidth;
    static HashMap<Predicate.Op, Predicate.Op> rev;
    static {
        rev = new HashMap<Predicate.Op, Predicate.Op>();
        rev.put(Predicate.Op.NOT_EQUALS, Predicate.Op.EQUALS);
        rev.put(Predicate.Op.LESS_THAN, Predicate.Op.GREATER_THAN_OR_EQ);
        rev.put(Predicate.Op.LESS_THAN_OR_EQ, Predicate.Op.GREATER_THAN);
    }

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.sm  = 0;
        this.hist = new int[buckets];
        this.binWidth = 1.0 * (max - min + 1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    @Override
    public void addValue(Integer v) {
    	// some code goes here
        hist[(int)((v - min) / binWidth)] += 1;
        sm += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    @Override
    public double estimateSelectivity(Predicate.Op op, Integer v) {
    	// some code goes here
        int bin = (int)((v - min) / binWidth);
        switch (op) {
            case EQUALS:
                if (bin < 0 || bin >= buckets) return 0.0;
                return 1.0 * hist[bin] / binWidth / sm;
            case GREATER_THAN:
                if (bin < 0) return 1.0;
                else if (bin >= buckets) return 0.0;
                double res = 1.0 * hist[bin] / binWidth * (min + bin * binWidth - v);
                for (int i = bin + 1; i < buckets; i++) res += hist[i];
                return res / sm;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                return 1 - estimateSelectivity(rev.get(op), v);
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
