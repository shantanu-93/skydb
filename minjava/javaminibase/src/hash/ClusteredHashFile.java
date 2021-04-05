package hash;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import btree.PinPageException;
import global.AttrType;
import global.PageId;
import global.RID;
import heap.*;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.IOException;

public class ClusteredHashFile extends HashFile {
    private short tupleFldCnt;
    private AttrType[] tupleAttrType;
    private short[] tupleStrSizes;

    public ClusteredHashFile(String filename, short tupleFldCnt,
                              AttrType[] tupleAttrType, short[] tupleStrSizes) throws ConstructPageException, AddFileEntryException, GetFileEntryException, InvalidSlotNumberException, IOException {
        super(filename, 0,true);
        this.tupleFldCnt = tupleFldCnt;
        this.tupleAttrType = tupleAttrType;
        this.tupleStrSizes = tupleStrSizes;
    }

    public ClusteredHashFile(String filename, int targetUtilization, int keytype,
                             int keysize, short tupleFldCnt,
                             AttrType[] tupleAttrType, short[] tupleStrSizes) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        super(filename, targetUtilization, true);
        this.tupleFldCnt = tupleFldCnt;
        this.tupleAttrType = tupleAttrType;
        this.tupleStrSizes = tupleStrSizes;
    }

    public void insertRecord(KeyClass key, HashRecord data) throws IOException, ConstructPageException, InvalidSlotNumberException {
        int bucketKey = key.getKey() % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key.getKey() % (2 * headerPage.getNValue());
        }
        boolean dataPageFound = false;
        PageId dataPageId = null;
        int bucketPageId = buckets.get(bucketKey);
        ClusteredHashPage bucketPage = new ClusteredHashPage(new PageId(bucketPageId));
        RID tempRid = bucketPage.firstRecord();
        while (true) {
            while (tempRid != null) {
                byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                ClusteredHashRecord record = new ClusteredHashRecord(bytesRecord);
                dataPageFound = record.getKey() == key.getKey();
                if (dataPageFound) {
                    dataPageId = record.getPageId();
                    break;
                }
                tempRid = bucketPage.nextRecord(tempRid);
            }
            if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                bucketPage = new ClusteredHashPage(bucketPage.getNextPage());
                tempRid = bucketPage.firstRecord();
            } else {
                break;
            }

            if (dataPageFound) {
                break;
            }
        }
        if (dataPageFound) {
            ClusteredDataPage dataPage = new ClusteredDataPage(dataPageId);
            insertRecordToDataPage(dataPage, ((ClusteredHashRecord)data).getTupleBytes());
        } else {
            ClusteredDataPage dataPage = new ClusteredDataPage(HashPageType.CLUSTERED_DATA);
            insertRecordToDataPage(dataPage, ((ClusteredHashRecord)data).getTupleBytes());
            ((ClusteredHashRecord)data).setPageId(dataPage.getCurPage());
            super.insertRecord(key, data);
        }
    }

    public void insertRecordToDataPage(ClusteredDataPage page, byte[] data) throws IOException, ConstructPageException {
        RID recordRid = page.insertRecord(data);
        // record id is null if insufficient space
        boolean recordInserted = recordRid != null;

        // if insufficient space, try to insert in overflow page if it is already there, else add overflow page
        ClusteredDataPage prevPage = page;
        ClusteredDataPage nextPage;
        while (!recordInserted) {
            // overflow pages are already there
            PageId nextPageId = prevPage.getNextPage();
            nextPage = null;
            if (nextPageId.pid != HFPage.INVALID_PAGE) {
                nextPage = new ClusteredDataPage(nextPageId);
                recordRid = nextPage.insertRecord(data);
                recordInserted = recordRid != null;
                if (recordInserted) {
                    break;
                }
            } else {
                break;
            }
            // if even this page does not have space, try to go to next page
            if (nextPage != null) {
                prevPage = nextPage;
            } else {
                break;
            }
        }

        // if record still not inserted, add overflow page and insert into it
        if (!recordInserted) {
            ClusteredDataPage overflowPage = new ClusteredDataPage(HashPageType.CLUSTERED_DATA_OVERFLOW);
            prevPage.setNextPage(overflowPage.getCurPage());
            overflowPage.setPrevPage(prevPage.getCurPage());
            overflowPage.insertRecord(data);
        }
    }

    public void deleteRecord(KeyClass key, HashRecord data) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidTypeException, UnknowAttrType, TupleUtilsException {
        int bucketKey = key.getKey() % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key.getKey() % (2 * headerPage.getNValue());
        }
//        System.out.println("Key to delete: " + key + " Going to delete from bucket: " + bucketKey);
        boolean dataPageFound = false;
        PageId dataPageId = null;
        int bucketPageId = buckets.get(bucketKey);
        ClusteredHashPage bucketPage = new ClusteredHashPage(new PageId(bucketPageId));
        RID tempRid = bucketPage.firstRecord();
        while (true) {
            while (tempRid != null) {
                byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                ClusteredHashRecord record = new ClusteredHashRecord(bytesRecord);
                dataPageFound = record.getKey() == key.getKey();
                if (dataPageFound) {
                    dataPageId = record.getPageId();
                    break;
                }
                tempRid = bucketPage.nextRecord(tempRid);
            }
            if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                bucketPage = new ClusteredHashPage(bucketPage.getNextPage());
                tempRid = bucketPage.firstRecord();
            } else {
                break;
            }

            if (dataPageFound) {
                break;
            }
        }
        if (dataPageFound) {
            // try to find the record
            ClusteredDataPage dataPage = new ClusteredDataPage(dataPageId);
            ClusteredDataPage firstDataPage = dataPage;
            RID tempDataRid  = dataPage.firstRecord();
            ((ClusteredHashRecord)data).setPageId(dataPageId);
            boolean recordFound = false;
            while (true) {
                while (tempDataRid != null) {
                    Tuple tup = dataPage.getTupleFromSlot(tempDataRid.slotNo);
                    tup.setHdr(tupleFldCnt, tupleAttrType, tupleStrSizes);
                    recordFound = TupleUtils.Equal(((ClusteredHashRecord) data).getData(), tup, tupleAttrType, tupleFldCnt);
                    if (recordFound) {
                        System.out.print("Deleting Record: ");
                        tup.print(tupleAttrType);
                        dataPage.deleteRecord(tempDataRid);
                        break;
                    }
                    tempDataRid = dataPage.nextRecord(tempDataRid);
                }
                if (dataPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                    dataPage = new ClusteredDataPage(dataPage.getNextPage());
                    tempDataRid = dataPage.firstRecord();
                } else {
                    break;
                }
                if (recordFound) {
                    break;
                }
            }
            boolean allEmpty = true;
            ClusteredDataPage checkDataPage = firstDataPage;
            while (true) {
                if (checkDataPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                    checkDataPage = new ClusteredDataPage(checkDataPage.getNextPage());
                } else {
                    break;
                }
                if (!checkDataPage.empty()) {
                    allEmpty = false;
                    break;
                }
            }
            if (allEmpty) {
                super.deleteRecord(key, data);
            }
        }

    }

    public void printIndex() throws IOException, InvalidTupleSizeException, InvalidTypeException {
        for (int i = 0; i <= headerPage.getBucketCount(); i++) {
            PageId bucketPageId = new PageId(buckets.get(i));
            HashPage bucketPage = getHashPage(bucketPageId);
            System.out.println("Bucket: " + i);
            RID tempRid = bucketPage.firstRecord();
            while (true) {
                while (tempRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                    HashRecord record = getHashRecord(bytesRecord);
                    System.out.println(record.toString() + ", ");

                    ClusteredDataPage dataPage = new ClusteredDataPage(((ClusteredHashRecord)record).getPageId());
                    RID tempDataRid  = dataPage.firstRecord();
                    while (true) {
                        while (tempDataRid != null) {
                            Tuple tup = dataPage.getTupleFromSlot(tempDataRid.slotNo);
                            tup.setHdr(tupleFldCnt, tupleAttrType, tupleStrSizes);
                            tup.print(tupleAttrType);
                            tempDataRid = dataPage.nextRecord(tempDataRid);
                        }
                        if (dataPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                            dataPage = new ClusteredDataPage(dataPage.getNextPage());
                            tempDataRid = dataPage.firstRecord();
                        } else {
                            break;
                        }
                    }

                    tempRid = bucketPage.nextRecord(tempRid);
                }

                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//                    System.out.println("lol");
                    bucketPage = getHashPage(bucketPage.getNextPage());
//                    System.out.println(bucketPage.empty());
                    tempRid = bucketPage.firstRecord();
                } else {
                    break;
                }
            }
            System.out.println();
        }
    }

}
