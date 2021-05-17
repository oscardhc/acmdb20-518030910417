package simpledb;

import java.io.IOException;

public class SingleChildOperator extends Operator {

    protected DbIterator child;

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        super.close();
        super.open();
        child.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return null;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }


}
