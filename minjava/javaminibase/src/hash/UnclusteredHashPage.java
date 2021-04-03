package hash;

import btree.NodeType;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import javax.xml.soap.Node;
import java.io.IOException;

public class UnclusteredHashPage extends HFPage {

    public UnclusteredHashPage(PageId pageno)
    {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public UnclusteredHashPage(short pageType) throws ConstructPageException {
        super();
        try{
            Page apage=new Page();
            PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
            if (pageId==null)
                throw new ConstructPageException("new page failed");
            this.init(pageId, apage);
            setType(pageType);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new ConstructPageException("construct header page failed");

        }
    }

    public void compact() throws IOException {
        super.compact_slot_dir();
    }

    public void deleteRecord ( RID rid ) throws IOException, InvalidSlotNumberException {
        super.deleteRecord(rid);
        // if overflow page gets empty, delete the page
        if (this.getType() == PageType.HASH_OVERFLOW) {
            if (this.empty()) {
                if (this.getNextPage().pid != HFPage.INVALID_PAGE) {
                    UnclusteredHashPage nextPage = new UnclusteredHashPage(this.getNextPage());
                    nextPage.setPrevPage(this.getPrevPage());
                    UnclusteredHashPage prevPage = new UnclusteredHashPage(this.getPrevPage());
                    prevPage.setNextPage(this.getNextPage());
                } else {
                    UnclusteredHashPage prevPage = new UnclusteredHashPage(this.getPrevPage());
                    prevPage.setNextPage(new PageId(HFPage.INVALID_PAGE));
                }
            }
        }
    }

    public RID insertRecord(int key, RID rid) throws IOException {
        byte[] tempData = new UnclusteredHashRecord(key, rid).getBytesFromRecord();
        return super.insertRecord(tempData);
    }

    public int getPageCapacity() throws IOException, InvalidSlotNumberException {
        int capacity = 0;
        System.out.println("blah: " + this.getSlotCnt());
        RID tempRid = insertRecord(0, new RID(new PageId(0), 0));

        capacity++;
        while (tempRid != null) {
            tempRid = insertRecord(0, new RID(new PageId(0), 0));
            capacity++;
        }
        System.out.println("blah: " + this.available_space());
        this.deleteRecord(this.firstRecord());
        System.out.println("blah: " + this.available_space());
        return --capacity;
    }

    public byte[] getBytesFromSlot(int slotNo) throws IOException {
        int slotOffset = getSlotOffset(slotNo);
        int slotLength = getSlotLength(slotNo);
        byte[] tempData = new byte[slotLength];
        System.arraycopy(data, slotOffset, tempData, 0, slotLength);
        return tempData;
    }

    PageId getPageId()
            throws IOException
    {
        return getCurPage();
    }

}
