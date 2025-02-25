package bufmgr;

import global.PageId;

public class FrameDesc {
    private PageId pageId;
    private int pinCount;
    private boolean dirty;
    //private String replacementPolicy;

    public FrameDesc() {
        this.pageId = new PageId(-1);
        this.pinCount = 0;
        this.dirty = false;
    }
    
    public FrameDesc(PageId pageId, int pinCount, boolean dirty) {
        this.pageId = pageId;
        this.pinCount = pinCount;
        this.dirty = dirty;
    }

    public PageId getPageId() {
        return this.pageId;
    }
    public void setPageId(PageId pageId) {
        this.pageId= pageId;
    }
    public void setPageId(int id) {
        PageId pageId = new PageId();
        pageId.pid = id;
        this.pageId= pageId;
    }
    public int getPinCount() {
        return pinCount;
    }
    public void setPinCount(int pinCount) {
        this.pinCount = pinCount;
    }
    public boolean isDirty() {
        return dirty;
    }
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }
    public boolean isFree() {
        return this.pageId.pid == -1;
    }
    public boolean isReplacable() {
        return this.pinCount == 0;
    }

}
