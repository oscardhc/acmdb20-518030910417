package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    class LockInfo {
        PageId pid;
        HashSet<TransactionId> shared = new HashSet<>();
        TransactionId exclusive = null;

        LockInfo(PageId pid) {
            this.pid = pid;
        }

        boolean tryGetLock(TransactionId tid, Permissions perm) {
            if (exclusive != null && exclusive != tid) return false;
            if (perm == Permissions.READ_WRITE) {
                if (shared.size() > 1) return false;
                if (shared.size() == 1) {
                    if (shared.contains(tid)) shared.clear();
                    else return false;
                }
                exclusive = tid;
            } else {
                shared.add(tid);
            }
            return true;
        }

        void freeLock(TransactionId tid) {
            if (exclusive != null && exclusive.equals(tid)) exclusive = null;
            else shared.remove(tid);
        }

    }

    private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edges = new ConcurrentHashMap<>();
    private HashSet<TransactionId> visited = new HashSet<>();
    synchronized void updateGraph(TransactionId tid, PageId pid) {
        HashSet<TransactionId> conflicts = new HashSet<>();
        LockInfo lock = locks.get(pid);
        synchronized (lock) {
            if (lock.exclusive != null) conflicts.add(lock.exclusive);
            conflicts.addAll(lock.shared);
        }
        edges.put(tid, conflicts);
    }
    synchronized void clearGraph(TransactionId tid) {
        edges.remove(tid);
    }
    synchronized boolean findCycle(TransactionId root) {
        visited.clear();
        return dfs(root, root);
    }
    synchronized boolean dfs(TransactionId cur, TransactionId root) {
        visited.add(cur);
        boolean ret = false;
        for (TransactionId nxt: edges.getOrDefault(cur, new HashSet<>())) {
            if (nxt.equals(root)) return true;
            if (!visited.contains(nxt)) ret = ret || dfs(nxt, root);
        }
        return ret;
    }

    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int npages = DEFAULT_PAGES;
    //    private ArrayList<Page> pages; // LRU
    private ConcurrentHashMap<PageId, Page> pages = new ConcurrentHashMap<>();

    private ConcurrentHashMap<PageId, LockInfo> locks = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> dirty = new ConcurrentHashMap<>();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        npages = numPages;
        pages = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        if (locks.get(pid) == null) locks.put(pid, new LockInfo(pid));
        LockInfo lock = locks.get(pid);
        while (true) {
            synchronized (lock) {
                if (lock.tryGetLock(tid, perm)) break;
            }
            updateGraph(tid, pid);
            if (findCycle(tid)) {
//                System.out.println("deadlock!!!!");
                throw new TransactionAbortedException();
            }
        }
        clearGraph(tid);

        dirty.putIfAbsent(tid, new HashSet<>());
        dirty.get(tid).add(pid);

        if (pages.get(pid) != null) {
            return pages.get(pid);
        } else {
            if (pages.size() >= npages) evictPage();
            Page pg = Database
                    .getCatalog()
                    .getDatabaseFile(pid.getTableId())
                    .readPage(pid);
            pages.put(pid, pg);
            pg.setBeforeImage();
            return pg;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        LockInfo lock = locks.get(pid);
        synchronized (lock) {
            lock.freeLock(tid);
        }
        dirty.get(tid).remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        LockInfo lock = locks.get(pid);
        synchronized (lock) {
            return lock.exclusive == tid || lock.shared.contains(tid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        dirty.putIfAbsent(tid, new HashSet<>());
        for (PageId pid: dirty.remove(tid)) {
            Page pg = pages.get(pid);
            if (pg != null && locks.get(pid).exclusive != null) {
                if (commit) {
                    if (pg.isDirty() != null) {
                        flushPage(pid);
                        pg.setBeforeImage();
                    }
                } else {
//                        pages.set(pages.indexOf(pg), pg.getBeforeImage());
                    pages.put(pid, pg.getBeforeImage());
                }
            }



            LockInfo lock = locks.get(pid);
            synchronized (lock) {
                lock.freeLock(tid);
            }
        }
    }


    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Database.getCatalog()
                .getDatabaseFile(tableId)
                .insertTuple(tid, t)
                .forEach(p -> {
                    p.markDirty(true, tid);
                    pages.put(p.getId(), p);
                });
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Database.getCatalog()
                .getDatabaseFile(t.getRecordId().getPageId().getTableId())
                .deleteTuple(tid, t)
                .forEach(p -> {
                    p.markDirty(true, tid);

//                    System.out.println(tid);
                    pages.put(p.getId(), p);
                });
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: pages.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
//        for (Page pg: pages) if (pg.getId().equals(pid)) {
        pages.remove(pid);
//            return;
//        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        Page pg = pages.get(pid);
        if (pg == null) return;
//        for (Page pg: pages) if (pg.getId().equals(pid)) {
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pg);
        pg.markDirty(false, null);
//        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        for (PageId pid: pages.keySet()) {
            Page pg = pages.get(pid);
            if (pg.isDirty() == null) {
                discardPage(pid);
                return;
            }
        }
        throw new DbException("");
    }

}
