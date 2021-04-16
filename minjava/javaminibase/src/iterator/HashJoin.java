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


class HashJoin{
    private   Heapfile  _innerhf;
    private Heapfile _outerHf;
    private Scan _innserScan;
    private Scan _outerScan;
    private String innerRelName;
    private String outterRelName;
    private HashMap<String, Integer> innerPartitionMap;
    private HashMap<String, Integer> outerPartitionMap;


    public HashJoin(String inRelName, String outRelName){
        innerRelName = inRelName;
        outterRelName = outRelName;

    }



    public void partition(){
        try{
            //Inner partitionning
            Heapfile innerHf = new Heapfile(innerRelName);
            Scan sc = innerHf.openScan();
            RID inRID = new RID();
            Tuple inTup = null;
            while ((inTup=sc.getNext(inRID))!= null){
                //call the functin
            }

            //Outer partitionning
            Heapfile outrHf = new Heapfile(innerRelName);
            Scan outSc = innerHf.openScan();
            RID outRID = new RID();
            Tuple outTup = null;
            while ((outTup=sc.getNext(outRID))!= null){
                //call the hash functin
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public int hashFunction(){
        //implement hash function
        return 0;
    }

    public Tuple get_next(){
        //get the result the join
        return null;
    }
}
