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

        //read the page  (using the appropriate method from {diskmgr} package)
        if(!emptyPage) {
          
          DB db = new DB();
          try{
            db.read_page(pin_pgid, bufPool[freeFrame]);
          }
          catch(Exception e) { //fix throwing errors here
            System.out.println("Error reading page from disk");
          }
        }

        //update file descriptor
        frameDesc[freeFrame].setPageId(pin_pgid);
        frameDesc[freeFrame].setPinCount(1);
        frameDesc[freeFrame].setDirty(false);

        //update hash table
        hashTable.insert(pin_pgid, freeFrame);

        //update FIFO queue
        fifoQueue.add(freeFrame);

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
   */

  public void unpinPage(PageId PageId_in_a_DB, boolean dirty) {

    int frameIndex = hashTable.getFrameNumber(PageId_in_a_DB);
    if ((frameIndex == -1)){
      //throw new PageUnpinnedException(null, "Page not in buffer pool");
    }

    //If pin_count=0 before this call, throw an exception to report error.
    //Further, if pin_count>0, this method should decrement it.
    int pinCount = frameDesc[frameIndex].getPinCount();
    if ( pinCount > 0) {
      frameDesc[frameIndex].setPinCount(pinCount - 1);
    } else {
      //throw new PageUnpinnedException(null, "Pin count is already 0");
    }
    
    //This method should be called with dirty==true if the client has modified the page
      //If so, this call should set the dirty bit for this frame.
    if(dirty) {
      frameDesc[frameIndex].setDirty(true);
    } else {
      frameDesc[frameIndex].setDirty(false);
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
    
    DB db = new DB();
    try {
      //PageId pageId = new PageId();
      db.allocate_page(firstpage, howmany); //firstpage needs to be PageID howwww to do thiss?????:
      //solution on piazza: You create a new PageId(), and pin it to firstpage still confused tho
    } catch (Exception e) {
      //ask DB to deallocate all these pages, and return null.
      return null;
    }
    int freeIndex = freeFrame();
    if (freeIndex == -1) {
      //ask DB to deallocate all these pages, and return null.
      db.deallocate_page(firstpage, howmany);
      return null;
    }
    //TODO:
    //read page
    //update file descriptor
    //update hash table
    //update FIFO queue
    //return pointer to page
      
  }


  /**
   * This method should be called to delete a page that is on disk.
   * This routine must call the method in diskmgr package to
   * deallocate the page.
   *
   * @param globalPageId the page number in the data base.
   */

  public void freePage(PageId globalPageId) {
      //YOUR CODE HERE
      int frameIndex = hashTable.getFrameNumber(globalPageId);
      if (frameIndex != -1) {
        if (frameDesc[frameIndex].getPinCount() > 0) {
          //throw new PagePinnedException(null, "Page is pinned");
        }
        //update hash table
        hashTable.remove(globalPageId);
        //update FIFO queue
        fifoQueue.remove(frameIndex);
        //update file descriptor
        frameDesc[frameIndex].setPageId(-1);
        frameDesc[frameIndex].setPinCount(0);
        frameDesc[frameIndex].setDirty(false);
        DB db = new DB();
        try {
          db.deallocate_page(globalPageId);
        } catch (Exception e) {
          //throw Exception
        }
      }
  }


  /**
   * Used to flush a particular page of the buffer pool to disk.
   * This method calls the write_page method of the diskmgr package.
   *
   * @param pageid the page number in the database.
   */

  public void flushPage(PageId pageid) {
    int frameIndex = hashTable.getFrameNumber(pageid);
    if(frameIndex != -1 && frameDesc[frameIndex].isDirty()) {
      DB db = new DB();
      try {
        db.write_page(pageid, bufPool[frameIndex]);
      } catch (Exception e) {
        //throw Exception
      }
      frameDesc[frameIndex].setDirty(false);
    }

  }

  /** Flushes all pages of the buffer pool to disk
   */

  public void flushAllPages() {
    for(int i = 0; i < this.numBuffers; i++) {
      int pageId = frameDesc[i].getPageId().pid;
      if(pageId != -1 &&frameDesc[i].isDirty()) {
        flushPage(frameDesc[i].getPageId());
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


  //returns the index of oldest unpinned frame to replace using the FIFO replacement policy
  private int getFrameToReplace() {
    for (int entry: fifoQueue) {
      if (frameDesc[entry].getPinCount() == 0) {
        fifoQueue.remove(entry);
        return entry;
      }
    }
    //throw exception if no frame to replace
    return -1;


  }
}

