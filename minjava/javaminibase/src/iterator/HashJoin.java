//   /**
//    * class constructor. set up the index scan.
//    * @param index type of the index (B_Index, Hash)
//    * @param relName name of the input relation
//    * @param indName name of the input index
//    * @param types array of types in this relation
//    * @param str_sizes array of string sizes (for attributes that are string)
//    * @param noInFlds number of fields in input tuple
//    * @param noOutFlds number of fields in output tuple
//    * @param outFlds fields to project
//    * @param selects conditions to apply, first one is primary
//    * @param fldNum field number of the indexed field
//    * @param indexOnly whether the answer requires only the key or the tuple
//    * @exception IndexException error from the lower layer
//    * @exception InvalidTypeException tuple type not valid
//    * @exception InvalidTupleSizeException tuple size not valid
//    * @exception UnknownIndexTypeException index type unknown
//    * @exception IOException from the lower layer
//    */


// try {
//     am = new IndexScan ( b_index, "sailors.in",
//         "BTreeIndex", Stypes, Ssizes, 4, 2,
//         Sprojection, null, 1, false);
// }catch(Exception e){
//     ;
// }


package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;


public class HashJoin{
    private   Heapfile  _innerhf;
    private Heapfile _outerHf;
    private Scan _innserScan;
    private Scan _outerScan;
    private String innerRelName;
    private String outterRelName;
    private HashMap<Integer, Heapfile> innerPartitionMap;
    private HashMap<Integer, Heapfile> outerPartitionMap;
    private int BUCKET_NUMBER=15;
    private String BUCKET_NAME_PREFIX = "bucket_";
    int hashAttr;
    ArrayList<Tuple> result;
    int value;





    public HashJoin(String inRelName, String outRelName, int attr, int value){
        innerRelName = inRelName;
        outterRelName = outRelName;
        hashAttr = attr;
        result = new ArrayList<>();
        this.value = value;
    }



    public void partition(){
        try{
            int hashVal = 0;
            //Inner partitionning
            Heapfile innerHf = new Heapfile(innerRelName);
            Scan sc = innerHf.openScan();
            RID inRID = new RID();
            Tuple inTup = null;
            while ((inTup=sc.getNext(inRID))!= null){
                //call the function
                 hashVal = hashFunction(inTup);
                 if(innerPartitionMap.get(hashVal)==null){
                     Heapfile hf = new Heapfile(BUCKET_NAME_PREFIX+hashVal);
                     innerPartitionMap.put(hashVal, hf);
                     insertRecordInBucket(inTup, hashVal, hf);
                 }else {
                     insertRecordInBucket(inTup,hashVal, innerPartitionMap.get(hashVal));
                 }

            }

            hashVal = 0;
            //Outer partitionning
            RID outRID = new RID();
            Tuple outTup = null;
            Heapfile outterHf = new Heapfile(outterRelName);
            Scan outerSc = innerHf.openScan();
            while ((outTup=outerSc.getNext(outRID))!= null){
                //call the hash functin
                hashVal = hashFunction(inTup);
                if(outerPartitionMap.get(hashVal)==null){
                    Heapfile hf = new Heapfile(BUCKET_NAME_PREFIX+hashVal);
                    outerPartitionMap.put(hashVal, hf);
                    insertRecordInBucket(outTup, hashVal, hf);
                }else{
                    insertRecordInBucket(outTup, hashVal, outerPartitionMap.get(hashVal));

                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }


    //HAsh function to be used by both tables.
    public int hashFunction(Tuple tple){
        //implement hash function
        int hashValue =0;
        AttrType[] type = tple.getTypes();

        try{
        //Integer hashing
            if(type[hashAttr].attrType== AttrType.attrInteger){
                int field = tple.getIntFld(hashAttr);
                hashValue = field % BUCKET_NUMBER;

            }
        }catch (Exception e){
            e.printStackTrace();
        }

        try{
            //String hashing
            if(type[hashAttr].attrType== AttrType.attrString){
                String field = tple.getStrFld(hashAttr);
                hashValue = Math.abs(field.hashCode())%BUCKET_NUMBER;
            }
        }catch (Exception e){

        }

        return hashValue;
    }

    public void insertRecordInBucket(Tuple tpl, int hashVal, Heapfile heap){
        //Insert
        try{
            byte [] tempBytes = tpl.returnTupleByteArray();
            heap.insertRecord(tempBytes);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public int hashFunctionInteger(int val){
        return val%BUCKET_NUMBER;
    }

    public int hashFunctionString(String s){
        return Math.abs(s.hashCode())%BUCKET_NUMBER;
    }

    public boolean tupleMatchOnField(Tuple tp1, Tuple tp2, int fieldNo, boolean isString){
        boolean equals = false;
        try{
            if(isString){
                String val1 = tp1.getStrFld(fieldNo);
                String val2 = tp2.getStrFld(fieldNo);
                equals = val1==val2;
            }else{
                int val1 = tp1.getIntFld(fieldNo);
                int val2 = tp2.getIntFld(fieldNo);

                equals = val1==val2;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return equals;
    }
    public void get_next(boolean isString){
        //get the result the join
        partition();
        int hashedValue = hashFunctionInteger(value);

        try{
            Heapfile innerHeapFile = innerPartitionMap.get(hashedValue);
            Heapfile outterHeapFile = outerPartitionMap.get(hashedValue);
            Scan innerSc = innerHeapFile.openScan();
            Scan outterSc = outterHeapFile.openScan();

            Tuple outterTuple = null;
            RID outRid = new RID();
            while ((outterTuple=outterSc.getNext(outRid))!=null){
                RID innerRid = new RID();
                Tuple innerTuple = null;
                while((innerTuple=innerSc.getNext(innerRid))!=null){
//                    check where they match
                    boolean match = tupleMatchOnField(innerTuple, outterTuple, hashAttr, isString);
                    if(match){
                        result.add(innerTuple);
                        result.add(outterTuple);
                    }

//                    add it to the list
                }
            }

            printAll();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void printAll(){
        AttrType[] type = new AttrType[2];
        AttrType attr1 = new AttrType(AttrType.attrInteger);
        AttrType attr2 = new AttrType(AttrType.attrInteger);
        type[0] = attr1;
        type[1] = attr2;
        try {
            for (Tuple x:result) {
                x.print(type);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }


}
