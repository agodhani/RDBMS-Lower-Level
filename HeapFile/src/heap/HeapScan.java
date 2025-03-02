package heap;

import java.util.*;

import bufmgr.*;
import global.* ;
import chainexception.ChainException;

/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {
   private HeapFile heapFile;
    private BufMgr bufMgr;
    private PageId currentPageId;
    private HFPage currentPage;
    private RID currentRid;
    private boolean isScanOpen;

  /**
   * Constructs a file scan by pinning the directoy header page and initializing
   * iterator fields.
      * @throws PageUnpinnedException 
      * @throws BufferPoolExceededException 
      */
  protected HeapScan(HeapFile hf) throws BufferPoolExceededException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    this.heapFile = hf;
    this.bufMgr = hf.bufMgr;
    this.currentPageId = null;
    this.currentPage = null;
    this.currentRid = null;
    this.isScanOpen = true;
    moveToFirstRecord();
    
  }

  private void moveToFirstRecord() throws BufferPoolExceededException, PageUnpinnedException {
    for (PageId pid : heapFile.freeSpaceMap.keySet()) {
        Page page = new Page();
        bufMgr.pinPage(pid, page, false);
        currentPage = new HFPage(page);
        currentPageId = pid;
        currentRid = currentPage.firstRecord();
        if (currentRid != null) {
            return;
        }
        bufMgr.unpinPage(pid, false);
    }
}

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {
    //PUT YOUR CODE HERE
    close();
  }

  /**
   * Closes the file scan, releasing any pinned pages.
      * @throws PageUnpinnedException 
      */
     public void close() throws PageUnpinnedException {
    //PUT YOUR CODE HERE
    if (isScanOpen) {
      if (currentPageId != null) {
          bufMgr.unpinPage(currentPageId, false);
      }
      isScanOpen = false;
  }

  }

  /**
   * Returns true if there are more records to scan, false otherwise.
   */
  public boolean hasNext() {
    //PUT YOUR CODE HERE
    return isScanOpen && currentRid != null;
  }

  /**
   * Gets the next record in the file scan.
   * 
   * @param rid output parameter that identifies the returned record
      * @throws PageUnpinnedException 
         * @throws BufferPoolExceededException 
            * @throws IllegalStateException if the scan has no more elements
            */
  public Tuple getNext(RID rid) throws PageUnpinnedException, BufferPoolExceededException {

    //PUT YOUR CODE HERE
    if (!hasNext()) {
        throw new IllegalStateException("No more records");
    }
    
    byte[] record = currentPage.selectRecord(currentRid);
    Tuple tuple = new Tuple(record, 0, record.length);
    rid.pageno = currentRid.pageno;
    rid.slotno = currentRid.slotno;
    
    // Move to the next record
    currentRid = currentPage.nextRecord(currentRid);
    
    while (currentRid == null) {
        bufMgr.unpinPage(currentPageId, false);
        currentPageId = currentPage.getNextPage();
        if (currentPageId.pid == -1) {
            currentPage = null;   
            return tuple;
        }
        Page nextPage = new Page();
        bufMgr.pinPage(currentPageId, nextPage, false);
        currentPage = new HFPage(nextPage);
        currentRid = currentPage.firstRecord();
    }
    return tuple;
  }

} // public class HeapScan implements GlobalConst
