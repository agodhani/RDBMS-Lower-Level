package heap;

import java.util.HashMap;

import bufmgr.*;
import chainexception.ChainException;
import diskmgr.*;
import global.*;



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
    public DiskMgr diskMgr;
    public HashMap<PageId, Integer> freeSpaceMap; 
    private int recordCount = 0;
    public PageId headerPageId;
    public PageId lastPageId;

  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile(String name) {
      
    this.fileName = name;
    this.bufMgr = Minibase.BufferManager;
    this.diskMgr = Minibase.DiskManager;
    this.freeSpaceMap = new HashMap<>();
    this.headerPageId = null;
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
    if (record.length > PAGE_SIZE) {
      throw new IllegalArgumentException("Record size exceeds page size");
    }
  //System.err.println("Inserting record of size " + record.length);
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
  //System.err.println("Inserted record into page " + targetPage.pid + " at slot " + rid.slotno);
  return rid;
}

  /**
   * Reads a record from the file, given its id.
      * @throws BufferPoolExceededException 
      * @throws PageUnpinnedException 
      * @throws IllegalArgumentException if the rid is invalid
  */
  public Tuple getRecord(RID rid) throws BufferPoolExceededException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    Page page = new Page();
    bufMgr.pinPage(rid.pageno, page, false);
    HFPage hfPage = new HFPage(page);
    byte[] record = hfPage.selectRecord(rid);
    Tuple tuple = new Tuple(record, 0, record.length);
    bufMgr.unpinPage(rid.pageno, false);

    return tuple;
  }

  /**
   * Updates the specified record in the heap file.
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            * @throws InvalidUpdateException 
                     * 
                     * @throws IllegalArgumentException if the rid or new record is invalid
                     */
  public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException{
    //PUT YOUR CODE HERE
    byte[] newRecord = newTuple.getTupleByteArray();

    Page page = new Page();
    bufMgr.pinPage(rid.pageno, page, false);
    HFPage hfPage = new HFPage(page);

    // Check length match
    short oldLength = hfPage.getSlotLength(rid.slotno);
    if (oldLength != (short)newRecord.length) {
        //throw new InvalidUpdateException(null, "Invalid record size");
        return false;
    }

    Tuple update = new Tuple(newRecord, 0, newRecord.length);
    hfPage.updateRecord(rid, update);

    bufMgr.unpinPage(rid.pageno, true);
    return true;
  

  }

  /**
   * Deletes the specified record from the heap file.
      * @throws PageUnpinnedException 
         * @throws BufferPoolExceededException 
            * 
            * @throws IllegalArgumentException if the rid is invalid
            */
  public boolean deleteRecord(RID rid) throws PageUnpinnedException, BufferPoolExceededException {
    //PUT YOUR CODE HERE
    Page page = new Page();
    bufMgr.pinPage(rid.pageno, page, false);
    HFPage hfPage = new HFPage(page);
    try {
        hfPage.deleteRecord(rid);
    } catch (IllegalArgumentException e) {
        bufMgr.unpinPage(rid.pageno, false);
        return false;
    }
    // Update free space map and record count
    freeSpaceMap.put(rid.pageno, (int)hfPage.getFreeSpace());
    recordCount--;
    bufMgr.unpinPage(rid.pageno, true);
    return true;

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
    //System.err.println("Finding page for record of size " + recordSize);
    //System.err.println("Free space map: " + freeSpaceMap);
    for (PageId pid : freeSpaceMap.keySet()) {
      //System.err.println("Checking page " + pid.pid + " with " + freeSpaceMap.get(pid) + " free bytes");
        if (freeSpaceMap.get(pid) >= recordSize + 4) {
            return pid;
        }
    }
    // If no existing page has enough space, allocate a new one
    //System.err.println(diskMgr);
    PageId newPageId;
    try {
      newPageId = diskMgr.allocate_page(1);
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
        hfPage.setNextPage(new PageId(-1));
        hfPage.setPrevPage(new PageId(-1));
        if (this.lastPageId == null) {
            this.headerPageId = newPageId;
            this.lastPageId = newPageId;
        } else {
            Page curLastPage = new Page();
            
            bufMgr.pinPage(this.lastPageId, curLastPage, false);
            HFPage curLastHFPage = new HFPage(curLastPage);
            curLastHFPage.setNextPage(newPageId);
            bufMgr.unpinPage(this.lastPageId, true);

            hfPage.setPrevPage(this.lastPageId);
            this.lastPageId = newPageId;
        }
       
        // Update freeSpaceMap
        freeSpaceMap.put(newPageId, (int)hfPage.getFreeSpace());
        bufMgr.unpinPage(newPageId, true);
        //System.err.println("Allocated new page " + newPageId.pid+" for record" + recordSize);
    } catch (Exception e) {
        throw new DiskMgrException(e, "Error initializing new HFPage");
    }

    return newPageId;

  }
} // public class HeapFile implements GlobalConst
