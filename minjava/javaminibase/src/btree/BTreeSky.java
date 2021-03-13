package btree;


import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import iterator.*;
import readdriver.ReadDriver;

import java.io.IOException;

public class BTreeSky extends Iterator implements GlobalConst {

  /////////////
  private AttrType[] attrType;
  private int attrCount;
  private short[] attrSizes;
  private Iterator iterator;
  private String relationName;
  private int[] pref_list;
  private int prefListLen;
  private BTreeFile[] bTreeIndexArray;
  private int n_pages;

  public boolean verbose = false;


  private String smallHeapFile = "btreeskyheapfile.in";

  public BTreeSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, Iterator am1,
                  String relationName, int[] pref_list, int pref_length_list, IndexFile[] index_file_list,
                  int n_pages) throws Exception {

    this.attrType = in1;
    this.attrCount = len_in1;
    this.attrSizes = t1_str_sizes;
    this.iterator = am1;
    this.relationName = relationName;
    this.pref_list = pref_list;
    this.prefListLen = pref_length_list;
    this.bTreeIndexArray = (BTreeFile[]) index_file_list;
    this.n_pages = n_pages;
  }

  @Override
  public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    return null;
  }

  @Override
  public void close() throws IOException, JoinsException, SortException, IndexException {

  }

  public void findBTreeSky() throws Exception {

    Tuple firstSkyTuple = BTreeUtil.getEmptyTuple(attrType, attrSizes);;
    RID firstSkyTupleRid = null;

    BTFileScan[] btFileScanArray = new BTFileScan[prefListLen];
    BTreeCustomScan[] tempScanArray = new BTreeCustomScan[prefListLen];

    for (int i = 0; i < prefListLen; i++) {
      btFileScanArray[i] = bTreeIndexArray[i].new_scan(null, null);
      tempScanArray[i] = new BTreeCustomScan("" + i);
    }

    boolean found = false;
    while (!found) {
      // iterate each index simultaneously
      for (int i = 0; i < prefListLen; i++) {

        KeyDataEntry nextScan = btFileScanArray[i].get_next();
        if (nextScan == null) {
          if (verbose)
            System.out.println("Index scan ended, exiting");
          break;
        }
        RID currRid = ((LeafData) nextScan.data).getData();
        int count = 0;
        for (int j = 0; j < prefListLen; j++) {
          // skip same index
          if (i != j) {
            if (tempScanArray[j].getIndexForRid(currRid) >= 0) {
              count++;
            }
          }
        }
        // current Rid also found in all other pref indexes
        if (count == (prefListLen - 1)) {
          found = true;
          firstSkyTupleRid = currRid;
          if (verbose) {
            System.out.println("firstSkyLineElement: " + firstSkyTupleRid);
          }
          break;
        }

        tempScanArray[i].addRecord(currRid);
      }
    }

    Heapfile parentHeapFile = new Heapfile(relationName);
    firstSkyTuple.tupleCopy(parentHeapFile.getRecord(firstSkyTupleRid));

    Heapfile smallHeapDataFile = new Heapfile(smallHeapFile);
    smallHeapDataFile.insertRecord(firstSkyTuple.returnTupleByteArray());
    BTreeCustomScan uniqArray = new BTreeCustomScan("uniq");

    for (int i = 0; i < prefListLen; i++) {
      BTreeCustomScan tempScanArray1 = tempScanArray[i];
      RID tempRid = new RID();
      Scan tempArrayScan = tempScanArray1.getHeapFile().openScan();
      Tuple tuple1;
      boolean done = false;
      while (!done) {
        tuple1 = tempArrayScan.getNext(tempRid);
        if (tuple1 == null) {
          done = true;
          break;
        }
        RID candidateRid = BTreeCustomScan.getRID(tuple1.returnTupleByteArray());

        if (candidateRid.equals(firstSkyTupleRid)) {
          break;
        }

        if (uniqArray.getIndexForRid(candidateRid) < 0) {
          Tuple parentHeapFileRecord = parentHeapFile.getRecord(candidateRid);
          smallHeapDataFile.insertRecord(parentHeapFileRecord.getTupleByteArray());
          uniqArray.addRecord(candidateRid);
        }

      }
      if (verbose) {
        System.out.println("");
      }


      tempArrayScan.closescan();

    }
    if (true) {
      System.out.println("Pruned Heap File Records Count : " + smallHeapDataFile.getRecCnt());
    }
    //run block nested loop skyline on the pruned data now

    FileScan fileScan = null;
    FldSpec[] fldSpecArray;
    fldSpecArray = new FldSpec[attrType.length];
    RelSpec relSpec = new RelSpec(RelSpec.outer);

    for (int i = 0; i < attrType.length; i++) {
      fldSpecArray[i] = new FldSpec(relSpec, i + 1);
    }

    try {
      fileScan =
              new FileScan(smallHeapFile, attrType, attrSizes, (short) attrType.length, attrType.length, fldSpecArray, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
//    SystemDefs.JavabaseBM.flushAllPages();

//    PCounter.initialize();

//    System.out.print("First Sky element is: ");
//    firstSkyLineElement.print(attrType);

//    System.out.println("runNestedLoopSky");
//    ReadDriver.runNestedLoopSky(prunedHeapFileName);

    System.out.println("runBNLSky");
    ReadDriver.runBNLSky(smallHeapFile);

//    System.out.println("runSortFirstSky");
//    ReadDriver.runSortFirstSky(prunedHeapFileName);

  }

}