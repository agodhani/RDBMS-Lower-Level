/* ... */

package bufmgr;

import java.io.*;
import java.util.*;
import diskmgr.*;
import global.*;

public class BufMgr implements GlobalConst{

  /**
   * Create the BufMgr object.
   * Allocate pages (frames) for the buffer pool in main memory and
   * make the buffer manage aware that the replacement policy is
   * specified by replacerArg.
   *
   * @param numbufs number of buffers in the buffer pool.
   * @param replacerArg name of the buffer replacement policy.
   */

  private Page[] bufPool;
  private FrameDesc[] frameDesc;
  private int numBuffers;
  private String replacerArg;  
  private myHashTable hashTable;
  private LinkedList<Integer> fifoQueue;

  public BufMgr(int numbufs, String replacerArg) {
    //initialize the buffer pool
    this.bufPool = new Page[numbufs];
    this.frameDesc = new FrameDesc[numbufs];
    this.numBuffers = numbufs;
    this.replacerArg = replacerArg;
    this.hashTable = new myHashTable();
    this.fifoQueue = new LinkedList<>();
    
    //allocate new Page and FrameDesc objects for each index in the buffer pool
    for (int i = 0; i < numbufs; i++) {
      this.bufPool[i] = new Page();
      this.frameDesc[i] = new FrameDesc();
    }
    

  }


  /**
   * Pin a page.
   * First check if this page is already in the buffer pool.
   * If it is, increment the pin_count and return a pointer to this
   * page.  If the pin_count was 0 before the call, the page was a
   * replacement candidate, but is no longer a candidate.
   * If the page is not in the pool, choose a frame (from the
   * set of replacement candidates) to hold this page, read the
   * page (using the appropriate method from {diskmgr} package) and pin it.
   * Also, must write out the old page in chosen frame if it is dirty
   * before reading new page.  (You can assume that emptyPage==false for
   * this assignment.)
   *
   * @param Page_Id_in_a_DB page number in the minibase.
   * @param page the pointer poit to the page.
   * @param emptyPage true (empty page); false (non-empty page)
      * @throws BufferPoolExceededException 
      */
   
     public void pinPage(PageId pin_pgid, Page page, boolean emptyPage) throws BufferPoolExceededException {
    //YOUR CODE HERE
    //call hash function to see if frame index exists (meaning page is in buffer pool)
    try {
      int frameIndex = hashTable.getFrameNumber(pin_pgid);
      //if page is in buffer pool, increment pin_count and return pointer to page
      if (frameIndex != -1) {
        frameDesc[frameIndex].setPinCount(frameDesc[frameIndex].getPinCount() + 1);
        page.setpage(bufPool[frameIndex].getpage());
        return;
      } else {
        //if page not in buffer pool, choose a frame to hold this page, 
        //find free frame 
        int freeFrame = freeFrame();
        if(freeFrame == -1) {
          //if no free frame, call FIFO replacer to find a frame to replace
          freeFrame = getFrameToReplace();
        }

        if (freeFrame == -1) {
          throw new BufferPoolExceededException(null, "Buffer Manager: No free frames");
        }

        //must write out the old page in chosen frame if it is dirty before reading new page. 
        if(frameDesc[freeFrame].isDirty()) { //if the page is dirty, write it to disk
          flushPage(frameDesc[freeFrame].getPageId());
        }
        
        if (frameDesc[freeFrame].getPageId().pid != -1) {
          hashTable.remove(frameDesc[freeFrame].getPageId());
          fifoQueue.remove((Integer)freeFrame);
      }
        

        //read the page  (using the appropriate method from {diskmgr} package)
        if(!emptyPage) {
          DB db = SystemDefs.JavabaseDB;
          //System.err.println("Reading page from disk: " + pin_pgid.pid + " buf val: " + bufPool[freeFrame].getpage());
          db.read_page(pin_pgid, bufPool[freeFrame]);
        }

        //update file descriptor
        PageId pig = new PageId(pin_pgid.pid);
        frameDesc[freeFrame].setPageId(pig);
        frameDesc[freeFrame].setPinCount(1);
        frameDesc[freeFrame].setDirty(false);

        //update hash table
        hashTable.insert(pig, freeFrame);

        //update FIFO queue
        //fifoQueue.add(freeFrame);

        //return pointer to page
        page.setpage(bufPool[freeFrame].getpage());
    }
    } catch (Exception e) {
      throw new BufferPoolExceededException(e, "Buffer Manager: pinPage() failed.");
    }

  }


  /**
   * Unpin a page specified by a pageId.
   * This method should be called with dirty==true if the client has
   * modified the page.  If so, this call should set the dirty bit
   * for this frame.  Further, if pin_count>0, this method should
   * decrement it. If pin_count=0 before this call, throw an exception
   * to report error.  (For testing purposes, we ask you to throw
   * an exception named PageUnpinnedException in case of error.)
   *
   * @param globalPageId_in_a_DB page number in the minibase.
   * @param dirty the dirty bit of the frame
      * @throws PageUnpinnedException 
      */
   
     public void unpinPage(PageId PageId_in_a_DB, boolean dirty) throws PageUnpinnedException {
    try {

    int frameIndex = hashTable.getFrameNumber(PageId_in_a_DB);
    if ((frameIndex == -1)){
      throw new HashEntryNotFoundException(null, "Page not in buffer pool");
    }

    //If pin_count=0 before this call, throw an exception to report error.
    //Further, if pin_count>0, this method should decrement it.
    int pinCount = frameDesc[frameIndex].getPinCount();
    if ( pinCount > 0) {
      frameDesc[frameIndex].setPinCount(pinCount - 1);
      if (frameDesc[frameIndex].getPinCount() == 0) {
        // Add frame back into the FIFO queue
        if (!fifoQueue.contains(frameIndex)) {
            fifoQueue.add(frameIndex);
        }
    }
    
    } else {
      throw new PageUnpinnedException(null, "Pin count is already 0");
    }
    
    //This method should be called with dirty==true if the client has modified the page
      //If so, this call should set the dirty bit for this frame.
      if (dirty) {
        frameDesc[frameIndex].setDirty(true);
      }
    
  } catch (Exception e) {
    throw new PageUnpinnedException(e, "Buffer Manager: unpinPage() failed");
  }
  }


  /**
   * Allocate new pages.
   * Call DB object to allocate a run of new pages and
   * find a frame in the buffer pool for the first page
   * and pin it. (This call allows a client of the Buffer Manager
   * to allocate pages on disk.) If buffer is full, i.e., you
   * can't find a frame for the first page, ask DB to deallocate
   * all these pages, and return null.
   *
   * @param firstpage the address of the first page.
   * @param howmany total number of allocated new pages.
   *
   * @return the first page id of the new pages.  null, if error.
   */

  public PageId newPage(Page firstpage, int howmany) {
    //Allocate new pages.
    //Call DB object to allocate a run of new pages and find a frame in the buffer pool for the first page and pin it.
    
    DB db = SystemDefs.JavabaseDB;
    PageId firstpageID = new PageId();
    try {
      db.allocate_page(firstpageID, howmany); 
      //solution on piazza: You create a new PageId(), and pin it to firstpage still confused tho
      pinPage(firstpageID, firstpage, false);
      //System.err.println("creating new page and page ID: " + firstpageID.pid);
      return firstpageID;
    } catch (Exception e) {
      //ask DB to deallocate all these pages, and return null.
      return null;
    }  
  }


  /**
   * This method should be called to delete a page that is on disk.
   * This routine must call the method in diskmgr package to
   * deallocate the page.
   *
   * @param globalPageId the page number in the data base.
      * @throws PagePinnedException 
      */
   
    public void freePage(PageId globalPageId) throws PagePinnedException {
    //YOUR CODE HERE
    
    try {
      int frameIndex = hashTable.getFrameNumber(globalPageId);
      if (frameIndex != -1) {
        if (frameDesc[frameIndex].getPinCount() > 1) {
          //System.err.println(frameDesc[frameIndex].toString());
          throw new PagePinnedException(null, "Page is pinned");
        } 
        if(frameDesc[frameIndex].getPinCount() == 1) {
          unpinPage(frameDesc[frameIndex].getPageId(), frameDesc[frameIndex].isDirty());
        }
        if(frameDesc[frameIndex].isDirty()) {
          flushPage(globalPageId);
        }

        //update hash table
        hashTable.remove(globalPageId);
        //update FIFO queue
        fifoQueue.remove((Integer)frameIndex);
        //update file descriptor
        frameDesc[frameIndex].setPageId(new PageId(-1));
        frameDesc[frameIndex].setPinCount(0);
        frameDesc[frameIndex].setDirty(false);
        DB db = SystemDefs.JavabaseDB;
        try {
          db.deallocate_page(globalPageId);
        } catch (Exception e) {
          throw new DiskMgrException(e, "Buffer Manager: dellocate failed"); 
        }
      } else {
        SystemDefs.JavabaseDB.deallocate_page(globalPageId);
        //throw new HashEntryNotFoundException(null, "Page not found in hash table");
      }
    } catch (Exception e) {
      throw new PagePinnedException(e, "Buffer Manager: freePage() failed");
    }

      
  }


  /**
   * Used to flush a particular page of the buffer pool to disk.
   * This method calls the write_page method of the diskmgr package.
   *
   * @param pageid the page number in the database.
      * @throws DiskMgrException 
      */
   
  public void flushPage(PageId pageid) throws DiskMgrException {
    int frameIndex = hashTable.getFrameNumber(pageid);
    if(frameIndex != -1) {
      DB db = SystemDefs.JavabaseDB;
      try {
        db.write_page(pageid, bufPool[frameIndex]);
        frameDesc[frameIndex].setDirty(false);
        //System.err.println("flushing page: " + pageid.pid+ " frame index: " + frameIndex);
      } catch (Exception e) {
        throw new DiskMgrException(null, "Buffer Mangager: write page failed");
      }
      
    }

  }

  /** Flushes all pages of the buffer pool to disk
     * @throws DiskMgrException 
     */
  
    public void flushAllPages() throws DiskMgrException {

    for(int i = 0; i < this.numBuffers; i++) {
      if(frameDesc[i].isDirty()) {
        try {
          flushPage(frameDesc[i].getPageId());
        } catch (Exception e) {
          throw new DiskMgrException(null, "Buffer Manager: flush page failed"); 
        }
      }
    }
  }


  /** Gets the total number of buffers.
   *
   * @return total number of buffer frames.
   */

  public int getNumBuffers() {
      return numBuffers;
  }


  /** Gets the total number of unpinned buffer frames.
   *
   * @return total number of unpinned buffer frames.
   */

  public int getNumUnpinnedBuffers() {
    int count = 0;
    for (int i = 0; i < frameDesc.length; i++) {
      if (frameDesc[i].getPinCount() == 0) {
        count++;
      }
    }
    return count; 
  }

  public int freeFrame() {
    for (int i = 0; i < frameDesc.length; i++) {
      if (frameDesc[i].getPageId().pid == -1) {
        return i;
      }
    }
    return -1;
  }


  private int getFrameToReplace() {
    Iterator<Integer> iter = fifoQueue.iterator();
    while (iter.hasNext()) {
        int frameIndex = iter.next();
        if (frameDesc[frameIndex].getPinCount() == 0) {
            iter.remove();  
            return frameIndex;
        }
    }
    return -1;
  }
}
