package hash;

import global.Convert;
import global.PageId;
import global.RID;
import heap.Tuple;

import java.io.IOException;

public class ClusteredHashRecord implements HashRecord {

    public static final int RECORD_SIZE = 8;

    private int key;
    private Tuple data;

    public void setPageId(PageId pageId) {
        this.pageId = pageId;
    }

    public PageId getPageId() {
        return pageId;
    }

    private PageId pageId;

    public ClusteredHashRecord(int key, Tuple data) throws ConstructPageException, IOException {
        this.key = key;
        this.data = data;
        this.pageId = new PageId();
    }

    public ClusteredHashRecord(byte[] recordBytes) throws IOException {
        this.key = Convert.getIntValue(0, recordBytes);
        int pid = Convert.getIntValue(4, recordBytes);
        this.pageId = new PageId(pid);
    }

    public byte[] getTupleBytes() {
        return data.getTupleByteArray();
    }

    public byte[] getBytesFromRecord() throws IOException {
        byte[] data = new byte[RECORD_SIZE];
        Convert.setIntValue(key, 0, data);
        Convert.setIntValue(pageId.pid, 4, data);
        return data;
    }

    public int getKey() {
        return key;
    }

    public boolean equals(HashRecord record) {
        return record.getKey() == key && ((ClusteredHashRecord)record).getPageId().pid == pageId.pid;
    }

    public Tuple getData() {
        return data;
    }

    public String toString() {
        return "" +key + "";
//        return "" +key + "" + ", <" + pageId.pid + ">";
//        return "< " + key + ", < " + rid.pageNo + ", " + rid.slotNo + " >>";
    }
}
