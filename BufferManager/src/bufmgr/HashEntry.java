package bufmgr;

import global.PageId;

public class HashEntry {
        PageId pageId;
        int frameNumber;

        HashEntry(PageId pageId, int frameNumber) {
            this.pageId = pageId;
            this.frameNumber = frameNumber;
        }
}
