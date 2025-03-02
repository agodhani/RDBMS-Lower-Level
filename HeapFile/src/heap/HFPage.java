//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package heap;

import global.Page;
import global.PageId;
import global.RID;
import global.GlobalConst;
import java.lang.*; 

class HFPage extends Page {
    protected static final int SLOT_CNT = 0;
    protected static final int USED_PTR = 2;
    protected static final int FREE_SPACE = 4;
    protected static final int PAGE_TYPE = 6;
    protected static final int PREV_PAGE = 8;
    protected static final int NEXT_PAGE = 12;
    protected static final int CUR_PAGE = 16;
    protected static final int HEADER_SIZE = 20;
    protected static final int SLOT_SIZE = 4;

    public HFPage() {
        this.initDefaults();
    }

    public HFPage(Page page) {
        super(page.getpage());
    }

    protected void initDefaults() {
        this.setShortValue((short)0, 0);
        this.setShortValue((short)1024, 2);
        this.setShortValue((short)1004, 4);
        this.setShortValue((short)0, 6);
        this.setIntValue(-1, 8);
        this.setIntValue(-1, 12);
        this.setIntValue(-1, 16);
    }

    public short getSlotCount() {
        return this.getShortValue(0);
    }

    public short getFreeSpace() {
        return this.getShortValue(4);
    }

    public short getType() {
        return this.getShortValue(6);
    }

    public void setType(short type) {
        this.setShortValue(type, 6);
    }

    public PageId getPrevPage() {
        return new PageId(this.getIntValue(8));
    }

    public void setPrevPage(PageId pageno) {
        this.setIntValue(pageno.pid, 8);
    }

    public PageId getNextPage() {
        return new PageId(this.getIntValue(12));
    }

    public void setNextPage(PageId pageno) {
        this.setIntValue(pageno.pid, 12);
    }

    public PageId getCurPage() {
        return new PageId(this.getIntValue(16));
    }

    public void setCurPage(PageId pageno) {
        this.setIntValue(pageno.pid, 16);
    }
        
    public short getSlotLength(int slotno) {
        return this.getShortValue(20 + slotno * 4);
    }

    public short getSlotOffset(int slotno) {
        return this.getShortValue(20 + slotno * 4 + 2);
    }

    public RID insertRecord(byte[] record) {
        short recLength = (short)record.length;
        int spaceNeeded = recLength + 4;
        short freeSpace = this.getShortValue(4);
        if (spaceNeeded > freeSpace) {
            return null;
        } else {
            short slotCnt = this.getShortValue(0);

            short i;
            for(i = 0; i < slotCnt; ++i) {
                short length = this.getSlotLength(i);
                if (length == -1) {
                    break;
                }
            }

            if (i == slotCnt) {
                freeSpace = (short)(freeSpace - spaceNeeded);
                this.setShortValue(freeSpace, 4);
                ++slotCnt;
                this.setShortValue(slotCnt, 0);
            } else {
                freeSpace -= recLength;
                this.setShortValue(freeSpace, 4);
            }

            short usedPtr = this.getShortValue(2);
            usedPtr -= recLength;
            this.setShortValue(usedPtr, 2);
            int slotpos = 20 + i * 4;
            this.setShortValue(recLength, slotpos);
            this.setShortValue(usedPtr, slotpos + 2);
            System.arraycopy(record, 0, this.data, usedPtr, recLength);
            return new RID(new PageId(this.getIntValue(16)), i);
        }
    }

    public byte[] selectRecord(RID rid) {
        short length = this.checkRID(rid);
        short offset = this.getSlotOffset(rid.slotNo);
        byte[] record = new byte[length];
        System.arraycopy(this.data, offset, record, 0, length);
        return record;
    }

    public void updateRecord(RID rid, Tuple record) {
        short length = this.checkRID(rid);
        if (record.getLength() != length) {
            throw new IllegalArgumentException("Invalid record size");
        } else {
            short offset = this.getSlotOffset(rid.slotNo);
            System.arraycopy(record.data, 0, this.data, offset, length);
        }
    }

    public void deleteRecord(RID rid) {
        short length = this.checkRID(rid);
        short offset = this.getSlotOffset(rid.slotNo);
        short usedPtr = this.getShortValue(2);
        short newSpot = (short)(usedPtr + length);
        short size = (short)(offset - usedPtr);
        System.arraycopy(this.data, usedPtr, this.data, newSpot, size);
        short slotCnt = this.getShortValue(0);
        int i = 0;

        int n;
        for(n = 20; i < slotCnt; n += 4) {
            if (this.getSlotLength(i) != -1) {
                short chkoffset = this.getSlotOffset(i);
                if (chkoffset < offset) {
                    chkoffset += length;
                    this.setShortValue(chkoffset, n + 2);
                }
            }

            ++i;
        }

        this.setShortValue(newSpot, 2);
        short freeSpace = this.getShortValue(4);
        freeSpace += length;
        this.setShortValue(freeSpace, 4);
        n = 20 + rid.slotNo * 4;
        this.setShortValue((short)-1, n);
        this.setShortValue((short)0, n + 2);
            }
        
            public RID firstRecord() {
        short slotCnt = this.getShortValue(0);

        int i;
        for(i = 0; i < slotCnt; ++i) {
            short length = this.getSlotLength(i);
            if (length != -1) {
                break;
            }
        }

        return i == slotCnt ? null : new RID(new PageId(this.getIntValue(16)), i);
    }

    public boolean hasNext(RID curRid) {
        int curPid = this.getIntValue(16);
        short slotCnt = this.getShortValue(0);
        if (curRid.pageNo.pid == curPid && curRid.slotNo >= 0 && curRid.slotNo <= slotCnt) {
            int i;
            for(i = curRid.slotNo + 1; i < slotCnt; ++i) {
                short length = this.getSlotLength(i);
                if (length != -1) {
                    break;
                }
            }

            return i != slotCnt;
        } else {
            throw new IllegalArgumentException("Invalid RID");
        }
    }

    public RID nextRecord(RID curRid) {
        int curPid = this.getIntValue(16);
        short slotCnt = this.getShortValue(0);
        if (curRid.pageNo.pid == curPid && curRid.slotNo >= 0 && curRid.slotNo <= slotCnt) {
            int i;
            for(i = curRid.slotNo + 1; i < slotCnt; ++i) {
                short length = this.getSlotLength(i);
                if (length != -1) {
                    break;
                }
            }

            return i == slotCnt ? null : new RID(new PageId(this.getIntValue(16)), i);
        } else {
            throw new IllegalArgumentException("Invalid RID");
        }
    }

    public void print() {
        short slotCnt = this.getShortValue(0);
        System.out.println("HFPage:");
        System.out.println("-------");
        System.out.println("  curPage   = " + this.getIntValue(16));
        System.out.println("  prevPage  = " + this.getIntValue(8));
        System.out.println("  nextPage  = " + this.getIntValue(12));
        System.out.println("  slotCnt   = " + slotCnt);
        System.out.println("  usedPtr   = " + this.getShortValue(2));
        System.out.println("  freeSpace = " + this.getShortValue(4));
        System.out.println("  pageType  = " + this.getShortValue(6));
        System.out.println("-------");
        int i = 0;

        for(int n = 20; i < slotCnt; n += 4) {
            System.out.println("slot #" + i + " offset = " + this.getShortValue(n));
            System.out.println("slot #" + i + " length = " + this.getShortValue(n + 2));
            ++i;
        }

    }

    protected short checkRID(RID rid) {
        int curPid = this.getIntValue(16);
        short slotCnt = this.getShortValue(0);
        if (rid.pageNo.pid == curPid && rid.slotNo >= 0 && rid.slotNo <= slotCnt) {
            short recLen = this.getSlotLength(rid.slotNo);
            if (recLen == -1) {
                throw new IllegalArgumentException("Empty slot");
            } else {
                return recLen;
            }
        } else {
            throw new IllegalArgumentException("Invalid RID");
        }
    }
                
    private int getIntValue(int i) {
        return ((data[i] & 0xFF) << 24) | 
        ((data[i + 1] & 0xFF) << 16) | 
        ((data[i + 2] & 0xFF) << 8) | 
        (data[i + 3] & 0xFF);
    }
        
    private short getShortValue(int i) {
        return (short) (((data[i] & 0xFF) << 8) | (data[i + 1] & 0xFF));
    }

    private void setShortValue(short s, int i) {
        data[i] = (byte) ((s >> 8) & 0xFF);
        data[i + 1] = (byte) (s & 0xFF);
    }
    private void setIntValue(int pid, int i) {
        data[i] = (byte) ((pid >> 24) & 0xFF);
        data[i + 1] = (byte) ((pid >> 16) & 0xFF);
        data[i + 2] = (byte) ((pid >> 8) & 0xFF);
        data[i + 3] = (byte) (pid & 0xFF);
    }
}
