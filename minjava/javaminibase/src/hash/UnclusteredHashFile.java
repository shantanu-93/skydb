package hash;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import java.io.IOException;

public class UnclusteredHashFile extends IndexFile {

    private HashHeaderPage headerPage;
    private PageId headerPageId;
    private String dbname;

    public UnclusteredHashFile(String filename)
            throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        headerPageId = get_file_entry(filename);
        if (headerPageId == null) //file not exist
        {
            headerPage = new HashHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.initialiseFirstTime();
            initialiseFile();
            headerPage.getAllBuckets();
//            headerPage.printAllSlotValues();

        } else {
            headerPage = new HashHeaderPage(headerPageId);
            System.out.println("opening existing");
            headerPage.initialiseAlreadyExisting();
            headerPage.getAllBuckets();
//            headerPage.printAllSlotValues();
//            System.out.println(headerPage.getCurrentUtilization());
        }
        printIndex();
//        headerPage.printAllSlotValues();

        dbname = new String(filename);

    }

    public void initialiseFile() throws ConstructPageException, IOException {
        for (int i = 0; i < headerPage.getNValue(); i++) {
            headerPage.insertNewBucket();
        }
    }

    public void insertRecord(int key, RID rid) throws IOException, ConstructPageException, InvalidSlotNumberException {
        int bucketKey = key % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key % (2 * headerPage.getNValue());
        }
        float util = headerPage.getCurrentUtilization();

        System.out.println(util);
        if (util <= headerPage.getTargetUtilization()) {
            PageId bucketPageId = new PageId(headerPage.getAllBuckets().get(bucketKey));
            UnclusteredHashPage bucketPage1 = new UnclusteredHashPage(bucketPageId);
            insertRecordToHashPage(bucketPage1, key, rid);
        } else {
            PageId bucketPageId1 = new PageId(headerPage.getAllBuckets().get(bucketKey));
            UnclusteredHashPage bucketPage1 = new UnclusteredHashPage(bucketPageId1);
            insertRecordToHashPage(bucketPage1, key, rid);
//            map.get(key).add(val); // overflow value
            // add one more bucket
            headerPage.insertNewBucket();
            PageId newBucketPageId = new PageId(headerPage.getAllBuckets().get(headerPage.getBucketCount()));
            UnclusteredHashPage newBucketPage = new UnclusteredHashPage(newBucketPageId);
            // rehash
            PageId bucketPageId = new PageId(headerPage.getAllBuckets().get(headerPage.getNextValue()));
            UnclusteredHashPage bucketPage = new UnclusteredHashPage(bucketPageId);
            RID tempRid = bucketPage.firstRecord();
            while (true) {
                while (tempRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                    UnclusteredHashRecord record = new UnclusteredHashRecord(bytesRecord);
                    int currVal = record.getKey();
                    int newKey = currVal % (2 * headerPage.getNValue());
                    if (newKey != headerPage.getNextValue()) {
                        System.out.println("moving record: " + record.getKey());
                        bucketPage.deleteRecord(tempRid);
                        insertRecordBytesToHashPage(newBucketPage, bytesRecord);
                    }
                    tempRid = bucketPage.nextRecord(tempRid);
                }

                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                    bucketPage = new UnclusteredHashPage(bucketPage.getNextPage());
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

    public void insertRecordBytesToHashPage(UnclusteredHashPage page, byte[] record) throws IOException, ConstructPageException {
        UnclusteredHashRecord tempRec = new UnclusteredHashRecord(record);
        insertRecordToHashPage(page, tempRec.getKey(), tempRec.getRid());
    }

    public void insertRecordToHashPage(UnclusteredHashPage page, int key, RID rid) throws IOException, ConstructPageException {
        RID recordRid = page.insertRecord(key, rid);
        // record id is null if insufficient space
        boolean recordInserted = recordRid != null;

        // if insufficient space, try to insert in overflow page if it is already there, else add overflow page
        UnclusteredHashPage prevPage = page;
        UnclusteredHashPage nextPage;
        while (!recordInserted) {
            // overflow pages are already there
            PageId nextPageId = prevPage.getNextPage();
            nextPage = null;
            if (nextPageId.pid != HFPage.INVALID_PAGE) {
                nextPage = new UnclusteredHashPage(nextPageId);
                recordRid = nextPage.insertRecord(key, rid);
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
            UnclusteredHashPage overflowPage = new UnclusteredHashPage(PageType.HASH_OVERFLOW);
            prevPage.setNextPage(overflowPage.getPageId());
            overflowPage.setPrevPage(prevPage.getPageId());
            overflowPage.insertRecord(key, rid);
        }
    }


    public void deleteRecord(int key, RID rid) throws IOException, InvalidSlotNumberException {
        int bucketKey = key % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key % (2 * headerPage.getNValue());
        }
        System.out.println("Key to delete: " + key + " Going to delete from bucket: " + bucketKey);
        PageId bucketPageId = new PageId(headerPage.getAllBuckets().get(bucketKey));
        UnclusteredHashPage bucketPage = new UnclusteredHashPage(bucketPageId);
        RID tempRid = bucketPage.firstRecord();
        boolean recordFound = false;
        while (true) {
            while (tempRid != null) {
                byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                UnclusteredHashRecord record = new UnclusteredHashRecord(bytesRecord);
                recordFound = record.getKey() == key && record.getRid().equals(rid);
                if (recordFound) {
                    System.out.println("Deleting Record: " + record.getKey());
                    bucketPage.deleteRecord(tempRid);
                    break;
                }
                tempRid = bucketPage.nextRecord(tempRid);
            }

            if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                bucketPage = new UnclusteredHashPage(bucketPage.getNextPage());
                tempRid = bucketPage.firstRecord();
            } else {
                break;
            }
            if (recordFound) {
                break;
            }
        }
    }

    public void printIndex() throws IOException {
        for (int i = 0; i <= headerPage.getBucketCount(); i++) {
            PageId bucketPageId = new PageId(headerPage.getAllBuckets().get(i));
            UnclusteredHashPage bucketPage = new UnclusteredHashPage(bucketPageId);
            System.out.println("Bucket: " + i);
            RID tempRid = bucketPage.firstRecord();
            while (true) {
                while (tempRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                    UnclusteredHashRecord record = new UnclusteredHashRecord(bytesRecord);
                    System.out.print(record.toString() + ", ");
                    tempRid = bucketPage.nextRecord(tempRid);
                }

                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                    System.out.println("lol");
                    bucketPage = new UnclusteredHashPage(bucketPage.getNextPage());
                    System.out.println(bucketPage.empty());
                    tempRid = bucketPage.firstRecord();
                } else {
                    break;
                }
            }
            System.out.println();
        }
        headerPage.getAllBuckets();
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
