package iterator;

import global.AttrType;
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

import btree.BTClusteredFileScan;
import btree.BTreeClusteredFile;
import btree.ConstructPageException;
import btree.FloatKey;
import btree.GetFileEntryException;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;
import btree.ClusteredLeafData;

public class TopK_NRAJoin {

    private AttrType[] in1, in2;
	private int len_in1, len_in2;
    private short[] t1_str_sizes, t2_str_sizes;
    private FldSpec joinAttr1, joinAttr2, mergeAttr1, mergeAttr2;
	private String relationName1, relationName2;
    private int k;
    int i = 1;
    private int n_pages;
    private BTreeClusteredFile[] file = new BTreeClusteredFile[2];
    private HashMap<Float, Float> lb = new HashMap<>();
    private HashMap<Float, Float> ub = new HashMap<>();
    float ub1=0.0f, ub2=0.0f;
    private KeyDataEntry[] data = new KeyDataEntry[2];
    private BTClusteredFileScan[] scan;
    float temp;
	boolean status = OK;

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
        try{
            file[0] = new BTreeClusteredFile(relationName1,(short) len_in1, in1, t1_str_sizes);
            file[1] = new BTreeClusteredFile(relationName2,(short) len_in2, in2, t2_str_sizes);
        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (PinPageException e) {
            e.printStackTrace();
        }

        System.out.println("\n -- Scanning BTreeClusteredFile");
        FloatKey key1 = new FloatKey(-1.0F);
        FloatKey key2 = new FloatKey(0.0F);
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

        computeLB_UB();
        
        while (data[0] != null || data[1] != null) {
            boolean val = sortByValue(lb, k);
            
            if(val){
                System.out.println("Algo complete");
                break;
            }
            
            computeLB_UB();
        }
    }

    private void computeLB_UB(){
        try {
            data[0] = scan[0].get_next();
            data[1] = scan[1].get_next();
            Tuple t1 = null, t2 = null;
            if (data[0] != null) t1 = (Tuple) ((ClusteredLeafData) data[0].data).getData();
            if (data[1] != null) t2 = (Tuple) ((ClusteredLeafData) data[1].data).getData();
            if (data[0] != null) {
                try {
                    ub2 = t2.getFloFld(mergeAttr2.offset);
                    if(!lb.containsKey(t1.getFloFld(joinAttr1.offset))){
                        lb.put(t1.getFloFld(joinAttr1.offset),t1.getFloFld(mergeAttr1.offset));
                        ub.put(t1.getFloFld(joinAttr1.offset), ub2 + t1.getFloFld(mergeAttr1.offset));
                    }else{
                        ub.put(t1.getFloFld(joinAttr1.offset), lb.get(t1.getFloFld(joinAttr1.offset)) + t1.getFloFld(mergeAttr1.offset));
                        lb.put(t1.getFloFld(joinAttr1.offset), lb.get(t1.getFloFld(joinAttr1.offset)) + t1.getFloFld(mergeAttr1.offset));
                    }
                } catch (FieldNumberOutOfBoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (data[1] != null) {
                try {
                    ub1 = t1.getFloFld(mergeAttr1.offset);
                    if(!lb.containsKey(t2.getFloFld(joinAttr2.offset))){
                        lb.put(t2.getFloFld(joinAttr2.offset),t2.getFloFld(mergeAttr2.offset));
                        ub.put(t2.getFloFld(joinAttr2.offset), ub1 + t2.getFloFld(mergeAttr2.offset));
                    }else{
                        ub.put(t2.getFloFld(joinAttr2.offset), lb.get(t2.getFloFld(joinAttr2.offset)) + t2.getFloFld(mergeAttr2.offset));
                        lb.put(t2.getFloFld(joinAttr2.offset), lb.get(t2.getFloFld(joinAttr2.offset)) + t2.getFloFld(mergeAttr2.offset));
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

    // function to sort hashmap by values
    public boolean sortByValue(HashMap<Float, Float> hm, int k)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<Float, Float> > list =
               new LinkedList<Map.Entry<Float, Float> >(hm.entrySet());
  
        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<Float, Float> >() {
            public int compare(Map.Entry<Float, Float> o1, 
                               Map.Entry<Float, Float> o2)
            {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });
  
        // put data from sorted list to hashmap 
        HashMap<Float, Float> temp = new LinkedHashMap<Float, Float>();
        for (Map.Entry<Float, Float> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }

        lb = temp;
        System.out.println("\nPass "+(i++));
        lb.entrySet().forEach(entry -> {
            System.out.println("Key: " + entry.getKey() + " LB: " + entry.getValue()+" UB: " + ub.get(entry.getKey()));
        });

        if(list.size() > k){
            if(list.get(k - 1).getValue() > ub.get(list.get(k).getKey()))
                return true;
            else
                return false;
        }
        return false;
    }
    
}
