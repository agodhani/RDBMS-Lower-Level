package heap;

import global.GlobalConst;
import global.Page;
import global.PageId;
import global.RID;

import java.util.ArrayList;
import java.util.List;

import bufmgr.*;
import diskmgr.*;



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
    private List<HFPage> pages; // List of all HFPages in the heap file
    private int recordCount;
  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile(String name) {
      this.fileName = name;
      this.pages = new ArrayList<>();
      this.recordCount = 0;
      
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
      //PUT YOUR CODE HERE
      super.finalize();
  }

  /**
   * Deletes the heap file from the database, freeing all of its pages.
   */
  public void deleteFile() {
    //PUT YOUR CODE HERE
    pages.clear();
  }

  /**
   * Inserts a new record into the file and returns its RID.
   * 
   * @throws IllegalArgumentException if the record is too large
   */
  public RID insertRecord(byte[] record) {
    //PUT YOUR CODE HERE
    for (HFPage page : pages) {
      if (page.getFreeSpace() > (short) record.length) {
          return page.insertRecord(record);
      }
  }

  // If no page has space, allocate a new page
  HFPage newPage = new HFPage();
  pages.add(newPage);
  return newPage.insertRecord(record);
  }

  /**
   * Reads a record from the file, given its id.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) {
    //PUT YOUR CODE HERE
  }

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) {
    //PUT YOUR CODE HERE
  }

  /**
   * Deletes the specified record from the heap file.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) {
    //PUT YOUR CODE HERE
  }

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {
    //PUT YOUR CODE HERE
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
  }

} // public class HeapFile implements GlobalConst
