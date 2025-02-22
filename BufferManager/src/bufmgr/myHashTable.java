import java.util.LinkedList;
import global.*;

public class myHashTable {

    private static final int HTSIZE = 101;
    private LinkedList<HashEntry>[] table;

    private static class HashEntry() {
        PageId pageId;
        int frameNumber;

        HashEntry(PageId pageId, int frameNumber) {
            this.pageId = pageId;
            this.frameNumber = frameNumber;
        }
    }
    
    public myHashTable() {
        table = new LinkedList[HTSIZE];
        for (int i = 0; i < HTSIZE; i++) {
            table[i] = new LinkedList<>();
        }
    }


    private int hash(PageId pageId) {
        int a = 3, b = 7;  // Arbitrary constants for hashing
        return ((a * pageId.pid + b) % HTSIZE);
    }

    public void insert(PageId pageId, int frameNumber) {
        int index = hash(pageId);
        for (HashEntry entry : table[index]) {
            if (entry.pageId.pid == pageId.pid) {
                entry.frameNumber = frameNumber; // Update existing entry
                return;
            }
        }
        table[index].add(new HashEntry(pageId, frameNumber));
    }

    public int getFrameNumber(PageId pageId) {
        int index = hash(pageId);
        for (HashEntry entry : table[index]) {
            if (entry.pageId.pid == pageId.pid) {
                return entry.frameNumber;
            }
        }
        return -1;

    }

    public void remove(PageId page) {
        int index = hash(page);
        for (HashEntry entry : table[index]) {
            if (entry.pageId.pid = page.pid) {
                table[index].remove();
            }
        }
    }

    public boolean contains(PageId page) {
        return getFrameNumber(page) != -1;
    }

}