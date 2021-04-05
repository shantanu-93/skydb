package hash;

import global.Convert;
import global.PageId;
import global.RID;

import java.io.IOException;

public class UnclusteredHashRecord implements HashRecord {
    public static final int RECORD_SIZE = 12;

    private int key;
    private RID rid;

    public UnclusteredHashRecord(int key, RID rid) {
        this.key = key;
        this.rid = rid;
    }

    public UnclusteredHashRecord(byte[] data) throws IOException {
        this.key = Convert.getIntValue(0, data);
        int pageId = Convert.getIntValue(4, data);
        int slotNo = Convert.getIntValue(8, data);
        this.rid = new RID(new PageId(pageId), slotNo);
    }

    public byte[] getBytesFromRecord() throws IOException {
        byte[] data = new byte[RECORD_SIZE];
        Convert.setIntValue(key, 0, data);
        Convert.setIntValue(rid.pageNo.pid, 4, data);
        Convert.setIntValue(rid.slotNo, 8, data);
        return data;
    }

    public boolean equals(HashRecord record) {
        return record.getKey() == key && ((UnclusteredHashRecord) record).getRid().equals(rid);
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public RID getRid() {
        return rid;
    }

    public void setRid(RID rid) {
        this.rid = rid;
    }

    public String toString() {
        return "" +key + "";
//        return "< " + key + ", < " + rid.pageNo + ", " + rid.slotNo + " >>";
    }
}
