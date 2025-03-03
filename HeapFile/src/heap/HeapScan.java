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
    System.err.println("Moving to first record"
    );
    for (PageId pid : heapFile.freeSpaceMap.keySet()) {
        Page page = new Page();
        bufMgr.pinPage(pid, page, false);
        HFPage currPage = new HFPage(page);
        RID rid  = currPage.firstRecord();
        if (rid != null) {
          this.currentPage = currPage;
          this.currentPageId = pid;
          this.currentRid = rid;
          
          return;
        }
        bufMgr.unpinPage(pid, false);
    }
    System.err.println("No first records found");
    this.currentPage = null;
    this.currentPageId = null;
    this.currentRid = null;
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
      currentPage = null;
      currentPageId = null;
      currentRid = null;
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
    System.err.println("Getting next record");
    if (!isScanOpen) {
        System.err.println("Scan is closed");
        //throw new IllegalStateException("No more records");
        return null;
    }
    while (currentPage != null &&currentRid == null) {
      System.err.println("Moving to next page");
      bufMgr.unpinPage(currentPageId, false);

      PageId nextPageId = currentPage.getNextPage();
      //currentPageId = nextPageId;
      
      if (nextPageId.pid == -1) {
          currentPage = null; 
          currentPageId = new PageId();  
          break;
          
      } else {
        Page nextPage = new Page();
        bufMgr.pinPage(nextPageId, nextPage, false);

        currentPage = new HFPage(nextPage);
        currentPageId = nextPageId;

        currentRid = currentPage.firstRecord();
      }
    }
    
    if (currentRid == null || currentPage == null) {
      return null;
    }

    
    byte[] record = currentPage.selectRecord(currentRid);
    Tuple tuple = new Tuple(record, 0, record.length);
    rid.pageno = currentRid.pageno;
    rid.slotno = currentRid.slotno;
    
    currentRid = currentPage.nextRecord(currentRid);
    
    
    return tuple;
  }

} // public class HeapScan implements GlobalConst
