package heap;

import global.GlobalConst;
import global.Page;
import global.PageId;
import global.RID;
import java.util.HashMap;

import bufmgr.*;
import diskmgr.DB;
import diskmgr.DiskMgrException;




/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is an unordered set of records, stored on a set of pages. This
 * class provides basic support for inserting, selecting, updating, and deleting
 * records. Temporary heap files are used for external sorting and in other
 * relational operators. A sequential scan of a heap file (via the Scan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

    private String fileName;
    private BufMgr bufMgr;
    private DB diskMgr;
    private HashMap<PageId, Integer> freeSpaceMap; 

  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile(String name) {
    this.fileName = name;
    this.bufMgr = new BufMgr(50, "Buffer Manager");
    this.diskMgr = new DB();
    this.freeSpaceMap = new HashMap<>();
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
      //PUT YOUR CODE HERE
      deleteFile();
  }

  /**
   * Deletes the heap file from the database, freeing all of its pages.
      * @throws DiskMgrException 
      */
     public void deleteFile() throws DiskMgrException {
    //PUT YOUR CODE HERE
      for (PageId pid : freeSpaceMap.keySet()) {
        try {
          diskMgr.deallocate_page(pid);
        } catch (Exception e) {
          throw new DiskMgrException(null, "deleteFile() failed, DiskMgr exception");
        }
      }
      freeSpaceMap.clear();
    }

  /**
   * Inserts a new record into the file and returns its RID.
      * @throws DiskMgrException 
         * @throws BufferPoolExceededException 
            * @throws PageUnpinnedException 
                     * 
                     * @throws IllegalArgumentException if the record is too large
                     */
                    public RID insertRecord(byte[] record) throws DiskMgrException, BufferPoolExceededException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    if (record.length > MINIBASE_PAGESIZE) {
      throw new IllegalArgumentException("Record size exceeds page size");
    }
  
  PageId targetPage = findPageForRecord(record.length);
  Page page = new Page();
  bufMgr.pinPage(targetPage, page, false);
  
  HFPage hfPage = new HFPage(page);
  RID rid = hfPage.insertRecord(record);
  
  freeSpaceMap.put(targetPage, (int) hfPage.getFreeSpace());
  bufMgr.unpinPage(targetPage, true);
  return rid;
}

  /**
   * Reads a record from the file, given its id.
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            * 
            * @throws IllegalArgumentException if the rid is invalid
            */
           public byte[] selectRecord(RID rid) throws BufferPoolExceededException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    Page page = new Page();
    bufMgr.pinPage(rid.pageNo, page, false);
    HFPage hfPage = new HFPage(page);
    byte[] record = hfPage.selectRecord(rid);
    bufMgr.unpinPage(rid.pageNo, false);
    return record;
  }

  /**
   * Updates the specified record in the heap file.
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            * 
            * @throws IllegalArgumentException if the rid or new record is invalid
            */
           public void updateRecord(RID rid, byte[] newRecord) throws BufferPoolExceededException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    Page page = new Page();
    bufMgr.pinPage(rid.pageNo, page, false);
    HFPage hfPage = new HFPage(page);
    Tuple update = new Tuple(newRecord, 0, newRecord.length);
    hfPage.updateRecord(rid, update);
    bufMgr.unpinPage(rid.pageNo, true);
  }

  /**
   * Deletes the specified record from the heap file.
      * @throws PageUnpinnedException 
         * @throws BufferPoolExceededException 
            * 
            * @throws IllegalArgumentException if the rid is invalid
            */
           public void deleteRecord(RID rid) throws PageUnpinnedException, BufferPoolExceededException {
    //PUT YOUR CODE HERE
    Page page = new Page();
    bufMgr.pinPage(rid.pageNo, page, false);
    HFPage hfPage = new HFPage(page);
    hfPage.deleteRecord(rid);
    freeSpaceMap.put(rid.pageNo, (int)hfPage.getFreeSpace());
    bufMgr.unpinPage(rid.pageNo, true);
  }

  /**
   * Gets the number of records in the file.
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            */
  public int getRecCnt() throws BufferPoolExceededException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    int count = 0;
    for (PageId pid : freeSpaceMap.keySet()) {
        Page page = new Page();
        bufMgr.pinPage(pid, page, false);
        HFPage hfPage = new HFPage(page);
        count += hfPage.getSlotCount();
        bufMgr.unpinPage(pid, false);
    }
    return count;
  }

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    //PUT YOUR CODE HERE
    return "HeapFile = " + fileName;
  }

  private PageId findPageForRecord(int recordSize) throws DiskMgrException {
    for (PageId pid : freeSpaceMap.keySet()) {
        if (freeSpaceMap.get(pid) >= recordSize) {
            return pid; // Return existing page with enough free space
        }
    }
    // If no existing page has enough space, allocate a new one
    PageId newPageId = new PageId();  // Create a new PageId object
    try {
    diskMgr.allocate_page(newPageId); // Pass it to allocate_page
    } catch (Exception e) {
      throw new DiskMgrException(e, "findPageForRecord() failed");
    }
    freeSpaceMap.put(newPageId, MINIBASE_PAGESIZE); // Initialize free space
    return newPageId;
  }
} // public class HeapFile implements GlobalConst
