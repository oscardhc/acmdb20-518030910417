package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
    }

    private HashMap<Object, Object> m = new HashMap<>();

    private Object reduce(Object _a, int b) {
        if (what==Op.AVG) {
            Pair<Integer, Integer> p =  (Pair<Integer, Integer>) _a;
            return new Pair<>(p.first + 1, p.second + b);
        } else {
            Integer a = (Integer) _a;
            if (what == Op.MIN) return Math.min(a, b);
            if (what == Op.MAX) return Math.max(a, b);
            if (what == Op.SUM) return a + b;
            if (what == Op.COUNT) return a + 1;
            return -1;
        }
    }
    private Object initial(int b) {
        if (what==Op.AVG) {
            return new Pair<>(1, b);
        } else {
            if (what == Op.COUNT) return 1;
            else return b;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field cur = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        IntField aval = (IntField)(tup.getField(afield));

        Object key = gbfield == NO_GROUPING ? -1 : cur;
        Object i = m.get(key);
        if (i == null) {
            m.put(key, initial(aval.getValue()));
        } else {
            m.put(key, reduce(i, aval.getValue()));
        }

    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        boolean nogroup = gbfield == NO_GROUPING;
        TupleDesc td = nogroup ? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        ArrayList<Tuple> hh = new ArrayList<>();
        for (Map.Entry<Object, Object> e: m.entrySet()) {
            Tuple t = new Tuple(td);
            int val = -1;
            if (what == Op.AVG) {
                Pair<Integer, Integer> p =  (Pair<Integer, Integer>) (e.getValue());
                val = p.second / p.first;
            } else {
                val = (Integer)(e.getValue());
            }
            if (!nogroup) t.setField(0, (Field)(e.getKey()));
            t.setField(nogroup ? 0 : 1, new IntField(val));
            hh.add(t);
        }
        return new TupleIterator(td, hh);
    }

}
