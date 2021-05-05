package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc desc;
    private RandomAccessFile stream;
    private int pageSize = BufferPool.getPageSize();

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        desc = td;
        try {
            stream = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException ignored) {

        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid instanceof HeapPageId) {
            HeapPageId hpid = (HeapPageId) pid;
            byte[] data = new byte[pageSize];
            try {
                stream.seek((long) pageSize * pid.pageNumber());
                stream.read(data);
                return new HeapPage(hpid, data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        stream.seek(page.getId().pageNumber() * pageSize);
        stream.write(page.getPageData(), 0, pageSize);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
            return (int)(stream.length()) / pageSize;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private ArrayList<Page> createList(Page p) {
        ArrayList<Page> a = new ArrayList<>();
        a.add(p);
        return a;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int np = numPages();
        for (int i = 0; i < np; i++) {
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            try {
                p.insertTuple(t);
                return createList(p);
            } catch (DbException e) {
            }
        }
        writePage(new HeapPage(new HeapPageId(getId(), np), HeapPage.createEmptyPageData()));

        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), np), Permissions.READ_WRITE);
        p.insertTuple(t);
        return createList(p);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        try {
            p.deleteTuple(t);
            return createList(p);
        } catch (DbException e) {
        }
        return createList(p);
    }

    class It implements DbFileIterator {

        int pgn, nxpgn;
        HeapPage pg, nxpg;
        TransactionId tid;
        Iterator<Tuple> it, nxit;

        It(TransactionId tid) {
            tid = tid;
            close();
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgn = 0;
            pg = (HeapPage) Database.getBufferPool()
                    .getPage(tid, new HeapPageId(getId(), pgn), Permissions.READ_ONLY);
            it = pg.iterator();
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) return false;
            if (it.hasNext()) return true;
            for (nxpgn = pgn + 1; nxpgn < numPages(); nxpgn++) {
                if ((nxpg = (HeapPage) Database.getBufferPool()
                        .getPage(tid, new HeapPageId(getId(), nxpgn), Permissions.READ_ONLY))
                        == null) return false;
                if ((nxit = nxpg.iterator()) == null) return false;
                if (!nxit.hasNext()) continue;
                return true;
            }
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) throw new NoSuchElementException();
            if (it.hasNext()) return it.next();
            if (nxpgn <= pgn || nxit == null) throw new NoSuchElementException();
            it = nxit;
            pg = nxpg;
            pgn = nxpgn;
            return it.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            pgn = nxpgn = -1;
            pg = nxpg = null;
            it = nxit = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new It(tid);
    }

}

