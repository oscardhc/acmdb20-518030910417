package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends SingleChildOperator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private boolean called = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called) return null;
        else {
            Tuple t = new Tuple(getTupleDesc());
            int cnt = 0;
            while (child.hasNext()) {
                try {
                    Database.getBufferPool().deleteTuple(tid, child.next());
                    cnt += 1;
                } catch (IOException e) {
                    throw new DbException("");
                }
            }
            t.setField(0, new IntField(cnt));
            called = true;
            return t;
        }
    }

}
