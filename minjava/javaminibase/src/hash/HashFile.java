package hash;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.IOException;
import java.util.HashMap;

public class HashFile extends IndexFile {

    protected HashHeaderPage headerPage;
    protected PageId headerPageId;
    protected String dbname;
    protected final HashMap<Integer, Integer> buckets;
    protected boolean isClustered;

    public HashFile(String filename, int targetUtilization, boolean isClustered)
            throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        this.headerPageId = get_file_entry(filename);
        this.buckets = new HashMap<>();
        this.isClustered = isClustered;
        if (headerPageId == null) //file not exist
        {
            headerPage = new HashHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.initialiseFirstTime(targetUtilization, isClustered);
            initialiseFile();
            getAllBuckets();
//            headerPage.printAllSlotValues();

        } else {
            headerPage = new HashHeaderPage(headerPageId);
            System.out.println("opening existing");
            headerPage.initialiseAlreadyExisting();
            getAllBuckets();
//            headerPage.printAllSlotValues();
//            System.out.println(headerPage.getCurrentUtilization());
        }
//        printIndex();
//        headerPage.printAllSlotValues();

        dbname = new String(filename);

    }

    public void initialiseFile() throws ConstructPageException, IOException {
        for (int i = 0; i < headerPage.getNValue(); i++) {
            insertNewBucket();
        }
    }

    public HashPage getHashPage(PageId pageId) {
        if (isClustered) {
            return new ClusteredHashPage(pageId);
        } else {
            return new UnclusteredHashPage(pageId);
        }
    }

    public HashPage getNewHashPage(short pageType) throws ConstructPageException {
        if (isClustered) {
            return new ClusteredHashPage(pageType);
        } else {
            return new UnclusteredHashPage(pageType);
        }
    }

    public HashRecord getHashRecord(byte[] recordBytes) throws IOException {
        if (isClustered) {
            return new ClusteredHashRecord(recordBytes);
        } else {
            return new UnclusteredHashRecord(recordBytes);
        }
    }

    public void insertRecord(KeyClass key, HashRecord data) throws IOException, ConstructPageException, InvalidSlotNumberException {
        int bucketKey = key.getKey() % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key.getKey() % (2 * headerPage.getNValue());
        }
        float util = headerPage.getCurrentUtilization();

//        System.out.println(util);
        if (util <= headerPage.getTargetUtilization()) {
            PageId bucketPageId = new PageId(buckets.get(bucketKey));
            insertRecordToHashPage(bucketPageId, key, data);
        } else {
            PageId bucketPageId1 = new PageId(buckets.get(bucketKey));
            insertRecordToHashPage(bucketPageId1, key, data);
            // add one more bucket
            insertNewBucket();
            PageId newBucketPageId = new PageId(buckets.get(headerPage.getBucketCount()));
            HashPage newBucketPage = getHashPage(newBucketPageId);
            // rehash
            PageId bucketPageId = new PageId(buckets.get(headerPage.getNextValue()));
            HashPage bucketPage = getHashPage(bucketPageId);
            RID tempRid = bucketPage.firstRecord();
            while (true) {
                while (tempRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                    HashRecord newRecord = getHashRecord(bytesRecord);
                    int currVal = newRecord.getKey();
                    int newKey = currVal % (2 * headerPage.getNValue());
                    if (newKey != headerPage.getNextValue()) {
//                        System.out.println("moving record: " + newRecord.getKey());
                        bucketPage.deleteRecord(tempRid);
                        newBucketPage.insertRecord(bytesRecord);
                    }
                    tempRid = bucketPage.nextRecord(tempRid);
                }

                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                    bucketPage = getHashPage(bucketPage.getNextPage());
                    tempRid = bucketPage.firstRecord();
                } else {
                    break;
                }
            }
            headerPage.setNextValue(headerPage.getNextValue() + 1);
            if (headerPage.getNextValue() == headerPage.getNValue()) {
                headerPage.setNValue(headerPage.getNValue() * 2);
                headerPage.setNextValue(0);
            }
        }
        headerPage.setNumOfRecords(headerPage.getNumOfRecords() + 1);
//        printIndex();
    }

//    public RID getNextFromBucket(int bucketKey, RID rid) throws IOException {
//        PageId bucketPageId = new PageId(headerPage.getAllBuckets().get(bucketKey));
//        UnclusteredHashPage bucketPage = new UnclusteredHashPage(bucketPageId);
//        if (rid.equals(new RID())) {
//            return bucketPage.firstRecord();
//        }
//        byte[] bytesRecord = bucketPage.getBytesFromSlot(rid.slotNo);
//        UnclusteredHashRecord record = new UnclusteredHashRecord(bytesRecord);
//        System.out.print(record.toString() + ", ");
//        RID nextRec = bucketPage.nextRecord(rid);
//        if (nextRec != null) {
//            return nextRec;
//        }
//        if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//            bucketPage = new UnclusteredHashPage(bucketPage.getNextPage());
//            return bucketPage.firstRecord();
//        }
//        return null;
//    }

    public void insertNewBucket() throws IOException, ConstructPageException {
        headerPage.setBucketCount(headerPage.getBucketCount()+1);
        HashPage page1 = getNewHashPage(HashPageType.HASH_BUCKET);

        byte[] tempData = new byte[8];
        Convert.setIntValue(headerPage.getBucketCount(), 0, tempData);
        int blah = page1.getCurPage().pid;
        Convert.setIntValue(blah, 4, tempData);

        RID recordRid = headerPage.insertRecord(tempData);
        boolean recordInserted = recordRid != null;
        // if insufficient space, try to insert in overflow page if it is already there, else add overflow page
        HashHeaderPage prevPage = headerPage;
        HashHeaderPage nextPage;
        while (!recordInserted) {
            // overflow pages are already there
            PageId nextPageId = prevPage.getNextPage();
            nextPage = null;
            if (nextPageId.pid != HFPage.INVALID_PAGE) {
                nextPage = new HashHeaderPage(nextPageId);
                recordRid = nextPage.insertRecord(tempData);
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
            System.out.println("inserting overflow page");
            HFPage overflowPage = new HashHeaderPage();
            prevPage.setNextPage(overflowPage.getCurPage());
            overflowPage.setPrevPage(prevPage.getCurPage());
            overflowPage.insertRecord(tempData);
        }

        buckets.put(headerPage.getBucketCount(), page1.getCurPage().pid);
    }

    public void insertRecordToHashPage(PageId pageId, KeyClass key, HashRecord data) throws IOException, ConstructPageException {
        HashPage page = getHashPage(pageId);
        RID recordRid = page.insertRecord(key, data);
        // record id is null if insufficient space
        boolean recordInserted = recordRid != null;

        // if insufficient space, try to insert in overflow page if it is already there, else add overflow page
        HashPage prevPage = page;
        HashPage nextPage;
        while (!recordInserted) {
            // overflow pages are already there
            PageId nextPageId = prevPage.getNextPage();
            nextPage = null;
            if (nextPageId.pid != HFPage.INVALID_PAGE) {
                nextPage = getHashPage(nextPageId);
                recordRid = nextPage.insertRecord(key, data);
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
            HashPage overflowPage = getNewHashPage(HashPageType.HASH_OVERFLOW);
            prevPage.setNextPage(overflowPage.getCurPage());
            overflowPage.setPrevPage(prevPage.getCurPage());
            overflowPage.insertRecord(key, data);
        }
    }

    public void getAllBuckets() throws IOException {
        int bucketSlotsStart = headerPage.getBucketSlotsStart();
        RID tempRid = new RID(headerPage.getPageId(), bucketSlotsStart);

        HashHeaderPage page = headerPage;

        while (true) {
            while (tempRid != null) {
                int key = Convert.getIntValue(page.getSlotOffset(tempRid.slotNo), page.getpage());
                int pageId = Convert.getIntValue(page.getSlotOffset(tempRid.slotNo) + 4, page.getpage());
                buckets.put(key, pageId);
                tempRid = page.nextRecord(tempRid);
            }

            if (page.getNextPage().pid != HFPage.INVALID_PAGE) {
                page = new HashHeaderPage(page.getNextPage());
                tempRid = page.firstRecord();
            } else {
                break;
            }
        }
    }

    public void deleteRecord(KeyClass key, HashRecord data) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidTypeException, UnknowAttrType, TupleUtilsException {
        int bucketKey = key.getKey() % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key.getKey() % (2 * headerPage.getNValue());
        }
//        System.out.println("Key to delete: " + key + " Going to delete from bucket: " + bucketKey);
        PageId bucketPageId = new PageId(buckets.get(bucketKey));
        HashPage bucketPage = getHashPage(bucketPageId);
        RID tempRid = bucketPage.firstRecord();
        boolean recordFound = false;
        while (true) {
            while (tempRid != null) {
                byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                HashRecord record = getHashRecord(bytesRecord);
                recordFound = record.equals(data);
                if (recordFound) {
//                    System.out.println("Deleting Record: " + record.getKey());
                    bucketPage.deleteRecord(tempRid);
                    break;
                }
                tempRid = bucketPage.nextRecord(tempRid);
            }

            if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                bucketPage = getHashPage(bucketPage.getNextPage());
                tempRid = bucketPage.firstRecord();
            } else {
                break;
            }
            if (recordFound) {
                break;
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
                    System.out.print(record.toString() + ", ");
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

    public void close()
            throws PageUnpinnedException,
            InvalidFrameNumberException,
            HashEntryNotFoundException,
            ReplacerException {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    public HashHeaderPage getHeaderPage() {
        return headerPage;
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private Page pinPage(PageId pageno)
            throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    private void add_file_entry(String fileName, PageId pageno)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageno)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageno)
            throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private void delete_file_entry(String filename)
            throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageno, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }
}
