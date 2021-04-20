package iterator;

import btree.BTClusteredFileScan;
import btree.BTreeClusteredFile;
import btree.KeyDataEntry;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tests.TestDriver.OK;

public class TopK_HashJoin {


  private AttrType[] in1, in2;
  private int len_in1, len_in2;
  private short[] t1_str_sizes, t2_str_sizes;
  private FldSpec joinAttr1, joinAttr2, mergeAttr1, mergeAttr2;
  private String relationName1, relationName2;
  private int k;
  private String oTable;
  boolean val;
  int i = 1;
  private int n_pages;
  private BTreeClusteredFile[] file = new BTreeClusteredFile[2];
//  private HashMap<Object, Integer[]> lub = new HashMap<Object, Integer[]>();
//  int ub1=0, ub2=0;
  private KeyDataEntry[] data = new KeyDataEntry[2];
  private BTClusteredFileScan[] scan;
  boolean status = OK;
//  List<Map.Entry<?, Integer[]> > list;
  private Heapfile f;
  int nColumns;
  short[] oAttrSize;
  AttrType[] oAttrTypes;
  String[] oAttrName;
  Tuple tuple1;
  short size;


  public TopK_HashJoin(AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1, FldSpec mergeAttr1, AttrType[] in2, int len_in2,
                       short[] t2_str_sizes, FldSpec joinAttr2, FldSpec mergeAttr2, String relationName1, String relationName2, int k, int n_pages, String oTable){
    this.relationName1 = relationName1;
    this.relationName2 = relationName2;
    this.in1 = in1;
    this.in2 = in2;
    this.len_in1 = len_in1;
    this.len_in2 = len_in2;
    this.t1_str_sizes = t1_str_sizes;
    this.t2_str_sizes = t2_str_sizes;
    this.joinAttr1 = joinAttr1;
    this.joinAttr2 = joinAttr2;
    this.mergeAttr1 = mergeAttr1;
    this.mergeAttr2 = mergeAttr2;
    this.k = k;
    this.n_pages = n_pages;
    this.oTable = oTable;
  }

  public void getHashBasesTopKJoin() {



    }


  //functon to get values by type
  private Object getField(Tuple t, int type, int offset){
    if(type == AttrType.attrString){
      try {
        return t.getStrFld(offset);
      } catch (FieldNumberOutOfBoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }else if(type == AttrType.attrInteger){
      try {
        return t.getIntFld(offset);
      } catch (FieldNumberOutOfBoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }else{
      try {
        return t.getFloFld(offset);
      } catch (FieldNumberOutOfBoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

}
