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

    public String fileName;
    public BufMgr bufMgr;
    public DB diskMgr;
    public HashMap<PageId, Integer> freeSpaceMap; 
    private int recordCount = 0;

  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile(String name) {
      
    this.fileName = name;
    this.bufMgr = new BufMgr(50, "FIFO");
    this.diskMgr = new DB();
    this.freeSpaceMap = new HashMap<>();
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
      //PUT YOUR CODE HERE
    if (fileName == null) {
        deleteFile();
    }
    super.finalize();

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
      recordCount = 0;
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

  if (rid == null) {
    bufMgr.unpinPage(targetPage, false);
    throw new IllegalArgumentException("Not enough space to insert record");
  }
  
  freeSpaceMap.put(targetPage, (int) hfPage.getFreeSpace());
  recordCount++;
  bufMgr.unpinPage(targetPage, true);
  return rid;
}

  /**
   * Reads a record from the file, given its id.
      * @throws BufferPoolExceededException 
      * @throws PageUnpinnedException 
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
            * @throws InvalidUpdateException 
                     * 
                     * @throws IllegalArgumentException if the rid or new record is invalid
                     */
           public void updateRecord(RID rid, byte[] newRecord) throws BufferPoolExceededException, PageUnpinnedException, InvalidUpdateException {
    //PUT YOUR CODE HERE

    Page page = new Page();
    bufMgr.pinPage(rid.pageNo, page, false);
    HFPage hfPage = new HFPage(page);

    // Check length match
    short oldLength = hfPage.getSlotLength(rid.slotNo);
    if (oldLength != (short)newRecord.length) {
        // The tests do expect an exception named "InvalidUpdateException"
        // if lengths differ. Let's do that to match test4() usage:
        throw new InvalidUpdateException(null, "Cannot change record size");
    }

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
    try {
        hfPage.deleteRecord(rid);
    } catch (IllegalArgumentException e) {
        bufMgr.unpinPage(rid.pageNo, false);
        throw new IllegalArgumentException("Invalid RID in deleteRecord()");
    }
    // Update free space map and record count
    freeSpaceMap.put(rid.pageNo, (int)hfPage.getFreeSpace());
    recordCount--;
    bufMgr.unpinPage(rid.pageNo, true);

  }

  /**
   * Gets the number of records in the file.
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            */
  public int getRecCnt() throws BufferPoolExceededException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    return recordCount;
  }

  /**
   * Initiates a sequential scan of the heap file.
      * @throws PageUnpinnedException 
      * @throws BufferPoolExceededException 
      */
  public HeapScan openScan() throws BufferPoolExceededException, PageUnpinnedException {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    //PUT YOUR CODE HERE
    return "HeapFile(" + (fileName == null ? "temp" : fileName) + ")";

  }

  private PageId findPageForRecord(int recordSize) throws DiskMgrException {
  
    for (PageId pid : freeSpaceMap.keySet()) {
        if (freeSpaceMap.get(pid) >= recordSize + 4) {
            return pid;
        }
    }
    // If no existing page has enough space, allocate a new one
    PageId newPageId = new PageId();
    try {
        diskMgr.allocate_page(newPageId);
    } catch (Exception e) {
        throw new DiskMgrException(e, "findPageForRecord() failed");
    }

    // Initialize the new page as an HFPage
    Page newPage = new Page();
    try {
        // Pin it as an empty page, then set up HFPage metadata
        bufMgr.pinPage(newPageId, newPage, /*emptyPage*/ true);
        HFPage hfPage = new HFPage(newPage);
        hfPage.initDefaults(); 
        hfPage.setCurPage(newPageId);
        // Update freeSpaceMap
        freeSpaceMap.put(newPageId, (int)hfPage.getFreeSpace());
        bufMgr.unpinPage(newPageId, true);
    } catch (Exception e) {
        throw new DiskMgrException(e, "Error initializing new HFPage");
    }

    return newPageId;

  }
} // public class HeapFile implements GlobalConst
