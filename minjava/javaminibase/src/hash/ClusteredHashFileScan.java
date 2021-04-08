package hash;

import global.AttrType;
import global.PageId;
import global.RID;
import heap.HFPage;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

public class ClusteredHashFileScan {
    ClusteredHashFile hashFile;
    KeyClass lowKey;
    KeyClass highKey;
    short tupleFldCnt;
    AttrType[] tupleAttrType;
    short[] tupleStrSizes;

    int bucketCount;
    int noOfBuckets;
    RID currRid;
    RID currDataRid;
    ClusteredHashPage bucketPage;

    boolean didFirst;
    boolean didFirstPage;

    ClusteredDataPage currPage;
    ClusteredHashRecord currRecord;



    ClusteredHashFileScan() {
        bucketCount = 0;
        didFirst = false;
    }

    public Tuple getNextTuple() throws IOException, InvalidTupleSizeException, InvalidTypeException {
        getNext();
        if (currDataRid == null) {
            return null;
        }
        Tuple tup = currPage.getTupleFromSlot(currDataRid.slotNo);
        tup.setHdr(tupleFldCnt, tupleAttrType, tupleStrSizes);
        return tup;
    }

    public RID getNext() throws IOException, InvalidTupleSizeException, InvalidTypeException {
        if (currDataRid == null) {
            currRecord = getNextRecord();
            System.out.println(currRecord);
            currPage = new ClusteredDataPage(currRecord.getPageId());
            currDataRid = currPage.firstRecord();
            return currDataRid;
        }
        currDataRid = currPage.nextRecord(currDataRid);
        if (currDataRid != null) {
//            System.out.println("lol");
            return currDataRid;
        } else if (currPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//            System.out.println("lol1");
            currPage = new ClusteredDataPage(currPage.getNextPage());
            currDataRid = currPage.firstRecord();
            return currDataRid;
        } else {
//            System.out.println("lol2");
            currRecord = getNextRecord();
            if (currRecord != null) {
                System.out.println(currRecord);
                currPage = new ClusteredDataPage(currRecord.getPageId());
                currDataRid = currPage.firstRecord();
                return currDataRid;
            } else {
                return null;
            }
        }
    }

    public ClusteredHashRecord getNextRecord() throws IOException {
        while (bucketCount <= hashFile.headerPage.getBucketCount()) {
//            System.out.println("Bucket: " + bucketCount);
            if (!didFirst) {
                PageId bucketPageId = new PageId(hashFile.buckets.get(bucketCount));
                bucketPage = new ClusteredHashPage(bucketPageId);
                currRid = bucketPage.firstRecord();
                didFirst = true;
            }

            while (true) {
                if (currRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(currRid.slotNo);
                    ClusteredHashRecord record = new ClusteredHashRecord(bytesRecord, hashFile.headerPage.getKeyType(), hashFile.headerPage.getKeySize());
                    currRid = bucketPage.nextRecord(currRid);
                    return record;
                }
                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//                    System.out.println("lol");
                    bucketPage = new ClusteredHashPage(bucketPage.getNextPage());;
//                    System.out.println(bucketPage.empty());
                    currRid = bucketPage.firstRecord();
                } else {
                    break;
                }
            }
//            System.out.println();
            didFirst = false;
            bucketCount++;
        }

        return null;
    }
}