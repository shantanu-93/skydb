package iterator;

import global.AttrType;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

import static tests.TestDriver.OK;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Attr;

import btree.AddFileEntryException;
import btree.BTClusteredFileScan;
import btree.BTreeClusteredFile;
import btree.ConstructPageException;
import btree.DeleteFileEntryException;
import btree.FloatKey;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import btree.ClusteredLeafData;

public class TopK_NRAJoin {

    private AttrType[] in1, in2;
	private int len_in1, len_in2;
    private short[] t1_str_sizes, t2_str_sizes;
    private FldSpec joinAttr1, joinAttr2, mergeAttr1, mergeAttr2;
	private String relationName1, relationName2;
    private int k;
    boolean val;
    int i = 1;
    private int n_pages;
    private BTreeClusteredFile[] file = new BTreeClusteredFile[2];
    private HashMap<Object, Integer[]> lub = new HashMap<Object, Integer[]>();
    int ub1=0, ub2=0;
    private KeyDataEntry[] data = new KeyDataEntry[2];
    private BTClusteredFileScan[] scan;
    boolean status = OK;
    List<Map.Entry<?, Integer[]> > list;

    public TopK_NRAJoin(){

    }

    public TopK_NRAJoin(AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1, FldSpec mergeAttr1, AttrType[] in2, int len_in2, 
        short[] t2_str_sizes, FldSpec joinAttr2, FldSpec mergeAttr2, String relationName1, String relationName2, int k,int n_pages){
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
            
    }

    public void computeTopK_NRA(){
            try {
                file[0] = new BTreeClusteredFile(relationName1, (short) in1.length, in1, t1_str_sizes);
                file[1] = new BTreeClusteredFile(relationName2, (short) in2.length, in2, t2_str_sizes);
            } catch (PinPageException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (GetFileEntryException e) {
                e.printStackTrace();
            } catch (ConstructPageException e) {
                e.printStackTrace();
            }


        // System.out.println("\n -- Scanning BTreeClusteredFile");
        IntegerKey key1 = new IntegerKey(-100000);
        IntegerKey key2 = new IntegerKey(0);
        
        scan = new BTClusteredFileScan[2];
        try {
            scan[0] = file[0].new_scan(key1, key2);
            scan[1] = file[1].new_scan(key1, key2);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeyNotMatchException e) {
            e.printStackTrace();
        } catch (IteratorException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (PinPageException e) {
            e.printStackTrace();
        } catch (UnpinPageException e) {
            e.printStackTrace();
        }

        computeLB_UB(in1[joinAttr1.offset - 1].attrType, in2[joinAttr2.offset - 1].attrType, in1[mergeAttr1.offset - 1].attrType, in2[mergeAttr2.offset - 1].attrType);

        
        while (data[0] != null || data[1] != null) {
            val = sortByValue(lub, k);
            
            if(val){
                System.out.println("---Top K Tuples---");
                for(int i = 0; i < k; i++){
                     System.out.println("Key: "+list.get(i).getKey()+" ["+-list.get(i).getValue()[0]+", "+-list.get(i).getValue()[1]+"]");
                }
                try {
                    file[0].destroyFile();
                    file[1].destroyFile();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                } catch (FreePageException e) {
                    e.printStackTrace();
                } catch (DeleteFileEntryException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                };
                
                break;
            }
            computeLB_UB(in1[joinAttr1.offset - 1].attrType, in2[joinAttr2.offset - 1].attrType, in1[mergeAttr1.offset - 1].attrType, in2[mergeAttr2.offset - 1].attrType);
        }
        if(!val){
            System.out.println("\n---Top K Tuples---");
                for(int i = 0; i < list.size(); i++){
                    System.out.println("Key: "+list.get(i).getKey()+" ["+-list.get(i).getValue()[2]+", "+-list.get(i).getValue()[3]+"]");
                }
                try {
                    file[0].destroyFile();
                    file[1].destroyFile();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                } catch (FreePageException e) {
                    e.printStackTrace();
                } catch (DeleteFileEntryException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                };
        }
    }

    private void computeLB_UB(int attrTypejoin_1, int attrTypejoin_2, int attrTypemerge_1, int attrTypemerge_2){

        Object t1join = null, t2join = null, t1merge = null, t2merge = null;
        
        try {
            data[0] = scan[0].get_next();
            data[1] = scan[1].get_next();
            Tuple t1 = null, t2 = null;
        
            if (data[0] != null) {
                t1 = (Tuple) ((ClusteredLeafData) data[0].data).getData();
                t1merge = getField(t1,attrTypemerge_1,mergeAttr1.offset);
                t1join = getField(t1, attrTypejoin_1, joinAttr1.offset);
            }
            if (data[1] != null){
                t2 = (Tuple) ((ClusteredLeafData) data[1].data).getData();
                t2join = getField(t2,attrTypejoin_1,joinAttr2.offset);   
                t2merge = getField(t2, attrTypemerge_2, mergeAttr2.offset);
            } 
            
            if (data[0] != null) {

                ub2  = (Integer) t2merge;
                if(!lub.containsKey(t1join)){
                    lub.put(t1join, new Integer[]{(Integer) t1merge, ub2 + (Integer) t1merge, (Integer) t1merge, 1});
                }else{
                    if(lub.get(t1join)[2] == 1){
                        lub.put(t1join, new Integer[]{lub.get(t1join)[0] + (Integer) t1merge, 
                            lub.get(t1join)[0] + (Integer) t1merge, (Integer) t1merge, lub.get(t1join)[3]});
                    }
                }

            }
            if (data[1] != null) {
                try {
                    ub1 = t1.getIntFld(mergeAttr1.offset);
                    if(!lub.containsKey(t2join)){
                        lub.put(t2join, new Integer[]{(Integer) t2merge, ub1 + (Integer) t2merge, 1, (Integer) t2merge});

                    }else{
                        if(lub.get(t2join)[3] == 1){
                            lub.put(t2join, new Integer[]{lub.get(t2join)[0] + (Integer) t2merge, 
                                lub.get(t2join)[0] + (Integer) t2merge, lub.get(t2join)[2], (Integer) t2merge});
                        }
                    }
                } catch (FieldNumberOutOfBoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (ScanIteratorException e) {
            e.printStackTrace();
        }
        
    }

    //functon to get values by type
    public Object getField(Tuple t, int type, int offset){
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

    // function to sort hashmap by values
    public boolean sortByValue(HashMap<?, Integer[]> hm, int k)
    {
        // Create a list from elements of HashMap
        list = new LinkedList<Map.Entry<?, Integer[]> >(hm.entrySet());
  
        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<?, Integer[]> >() {
            public int compare(Map.Entry<?, Integer[]> o1, 
                               Map.Entry<?, Integer[]> o2)
            {
                return (o1.getValue())[0].compareTo(o2.getValue()[0]);
            }
        });
  
        // put data from sorted list to hashmap 
        HashMap<Object, Integer[]> temp = new LinkedHashMap<Object, Integer[]>();
        for (Map.Entry<?, Integer[]> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }

        lub = temp;
        System.out.println("\nPass "+(i++));
        lub.entrySet().forEach(entry -> {
            System.out.println("Key: " + entry.getKey() + " LB: " + -entry.getValue()[0]+" UB: " + -entry.getValue()[1]);
        });

        if(list.size() > k){
            if(list.get(k).getValue()[1] >= list.get(k - 1).getValue()[0])
                return true;
            else
                return false;
        }
        return false;
    }
    
}
