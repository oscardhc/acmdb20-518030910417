package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
    }

    private HashMap<Object, Integer> m = new HashMap<>();

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field cur = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        Object key = gbfield == NO_GROUPING ? -1 : cur;
        Integer i = m.get(key);
        if (i == null) {
            m.put(key, 1);
        } else {
            m.put(key, i + 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        boolean nogroup = gbfield == NO_GROUPING;
        TupleDesc td = nogroup ? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        ArrayList<Tuple> hh = new ArrayList<>();
        for (Map.Entry<Object, Integer> e: m.entrySet()) {
            Tuple t = new Tuple(td);
            int val = e.getValue();
            if (!nogroup) t.setField(0, (IntField)(e.getKey()));
            t.setField(nogroup ? 0 :1, new IntField(val));
            hh.add(t);
        }
        return new TupleIterator(td, hh);
    }

}
