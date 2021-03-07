package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.ArrayList;
// import java.util.Iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;

/** 
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */

public class BlockNestedLoopsSky  extends Iterator 
{

  AttrType[] _in1;
  short in1_len;
  short[] _t1_str_sizes;
  Iterator am1_iter; 
  Heapfile hf;
  Heapfile tempHeap;
  int[] _pref_list;
  int _pref_list_length;
  int n_buff_pgs;
  int maxRecordSize;
  boolean spaceInWindow;
  ArrayList<Tuple> windowMemory;
  java.util.Iterator windowIterator;
  ArrayList<Tuple> finalOutput;
  java.util.Iterator finalOutputIter;
  RID id;
  Scan sc;
  Tuple outer_tuple;
  boolean done;
  boolean done2;
  Scan outer;
  String relationName;
  int n_buf_pgs; 
  boolean get_from_outer;


  
  
  public BlockNestedLoopsSky(   
        AttrType[] in1,
        int len_in1,
        short[] t1_str_sizes,
        Iterator am1,
        String relName,
        int[] pref_list,
        int pref_list_length,
        int n_pages
			   ) throws IOException,NestedLoopException, TupleUtilsException
    {
      
      _in1 = new AttrType[in1.length];
      System.arraycopy(in1,0,_in1,0,in1.length);
      
      in1_len = (short)len_in1;
            
      
      spaceInWindow=true;
      am1_iter = am1;

      n_buf_pgs = n_pages;
      done  = false;
      done2 = false;
      get_from_outer = true;
      _t1_str_sizes = t1_str_sizes;
      windowMemory = new ArrayList<Tuple>();
      windowIterator = windowMemory.iterator();
      _pref_list = pref_list;
      _pref_list_length = pref_list_length;
      n_buff_pgs = n_pages;
      relationName = (String)relName;

      outer_tuple = new Tuple();
      //Creating an output iterator
      finalOutput = new ArrayList<Tuple>();
      
      
      //Getting the maximum number of records on one page. 
      id = new RID();
      try {
	      hf = new Heapfile(relationName);
      }
      catch(Exception e) {
	      throw new NestedLoopException(e, "Create new heapfile failed.");
      }

      try{
        sc = hf.openScan();
      }catch(Exception e){
        throw new NestedLoopException(e, "openScan failed");
      }

      Tuple temptpl = null;

      try{
        
        temptpl = sc.getNextAndCountRecords(id);
        
        
      }catch(Exception e){
        throw new NestedLoopException(e, "Could not get number of records on page 1");
      }
      System.out.println(sc.getNumberOfRecordsPerOnePage());
      // while(temptpl!= null){
      //   System.out.println(sc.getNumberOfRecordsPerOnePage());

      //   //do nothing since it's counting the number of records on the first page
      //   ;
      // }

      // maxRecordSize = sc.getNumberOfRecordsPerOnePage()* n_buff_pgs;
      maxRecordSize = sc.getNumberOfRecordsPerOnePage()* n_buff_pgs;

      // System.out.println("In BlockNestedLoopsSky");
      // System.out.println(maxRecordSize);

      sc = null;
      hf = null;

      try {
        tempHeap = new Heapfile("temporary.in");

        
      } catch (Exception e) {
        //TODO: handle exception
        e.printStackTrace();
      }
      
      try {
	      hf = new Heapfile(relationName);
      }
      catch(Exception e) {
	      throw new NestedLoopException(e, "Create new heapfile failed.");
      }
    }
  
  /**  
   *@return The joined tuple is returned
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception TupleUtilsException exception from using tuple utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions
   */


   public Tuple performSkyline() throws IOException{

    
    int countTempR =0; 
    Scan tempScan = null;
    try {
      tempScan = new Scan(tempHeap);
    } catch (Exception e) {
      //TODO: handle exception
    } 
    if(tempHeap != null){

      
      try{
        countTempR = tempScan.getTotalNumberRecords();
        // System.out.println("The count in the heap file is: "+tempHeap.getRecCnt());
       }catch (Exception e) {
         e.printStackTrace();
       }
    }
    
    System.out.println("The count in the heap file is: "+tempScan.getTotalNumberRecords());

    if(tempHeap == null || countTempR<=0){

      try{
        blockSkyline();
        // blockSkyline("temporary.in");
        
      }catch(Exception e) {
        e.printStackTrace();
      }
    }

    System.out.println("The count in the heap file is: "+ tempScan.getTotalNumberRecords());
    //empty the window block
    windowMemory.clear();
    //deal with the temporary files
     if(tempHeap != null && countTempR>0){
       //if temporary file is not empty 
      //call the function again
      try{
        blockSkyline("temporary.in");
        // windowIterator = windowMemory.iterator();

      }catch(Exception e) {
        e.printStackTrace();
      }

      windowMemory.clear();
    
      if(countTempR>0){
        performSkyline();
      }
     }

     // finalOutputIter = new java.util.Iterator();  
     finalOutputIter = finalOutput.iterator();    
     return null;
   }


   public Tuple get_next(){

    if(finalOutput.isEmpty()){
      try {
        performSkyline();
      } catch (Exception e) {
        //TODO: handle exception
        e.printStackTrace();
      }
      
    }
     
     while(finalOutputIter.hasNext()){ 
       return (Tuple)finalOutputIter.next();
     }
     return null;
    }
  
    public ArrayList<Tuple> blockSkyline() throws IOException,NestedLoopException{
      // ArrayList<Tuple> result = new ArrayList<Tuple>();
      RID rid = new RID();
      do{
        // RID rid = new RID();
        if(done){
          break;
        }
        
        try{
          outer = hf.openScan();

        }catch(Exception e){
          throw new NestedLoopException(e, "openScan failed");
        }
        
        
        // RID rid = new RID();
        Tuple outerGetNext =null;
        // try{
        //   outerGetNext = outer.getNext(rid);
        //   // System.out.println("In get_next()");
        //     // System.out.println(outerGetNext);
        //     // if(true){
        //     //   break;
        //     // }
        // }catch(Exception e){
        //   throw new NestedLoopException(e, "Cannot get next tuple");
        // }
        try{

        
        while((outer_tuple=outer.getNext(rid))!=null){
          try{
            outer_tuple.setHdr((short) 2, _in1, _t1_str_sizes);
          }catch (Exception e) {
            e.printStackTrace();
          }
          // done = true;
          // outer = null;
        
          
        // outer_tuple = outer.getNext(rid));
            boolean windowDominate=false;
            boolean outerDominate = false;
          
              if(windowMemory.isEmpty()){
                
                windowMemory.add(outer_tuple);
      
              }else{
                
                int sizeResult = windowMemory.size();
                for(int i = 0; i<sizeResult ; i++){
                  // innerTuple, _in1, currentOuter, _in1, in1_len, t1_str_sizes_cls, pref_list_cls, pref_list_length_cls
                  //Checking domination
                  Tuple windowTuple = windowMemory.get(i);
                  try{

                    windowDominate = TupleUtils.Dominates(windowTuple,_in1,outer_tuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);// window dominate outer
                  }catch(Exception e) {
                    e.printStackTrace();
                  }

                  try {
                    outerDominate = TupleUtils.Dominates(outer_tuple,_in1,windowTuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);//outer dominate window

                  } catch (Exception e) {
                    e.printStackTrace();

                  }

                  // System.out.println(windowDominate+" and "+ outerDominate);

                  if(windowDominate){
                    try {
                      hf.deleteRecord(rid);//delete record from the heap file
                    } catch (Exception e) {
                      e.printStackTrace();
                    }
                    
                  }

                  if(outerDominate){

                    if(windowMemory.size()>=maxRecordSize){
                      //remove reocrd from window and set spaceWindow to false
                      windowMemory.remove(windowTuple);
                      spaceInWindow = false;
                    }if(windowTuple.size()<maxRecordSize){
                      windowMemory.remove(windowTuple);
                    }
                    
                    if(spaceInWindow){//if there is still enough block memory
                      
                      if(!windowMemory.contains(outer_tuple)){
                        windowMemory.add(outer_tuple);
                        
                      }
     
                    }else{
                      //Write into the temporary heapfile
                      byte [] outer_bytes=outer_tuple.returnTupleByteArray();
                      try {
                        tempHeap.insertRecord(outer_bytes);
                      } catch (Exception e) {
                        //TODO: handle exception
                        e.printStackTrace();
                      }
                      
                    }
                  }
                
                //if outer_tuple neither dominate nor is dominated
                if(!windowDominate && !outerDominate){


                  if(windowMemory.size()>=maxRecordSize){
                    spaceInWindow= false;
                    System.out.println("There is space");

                  }

                  if(spaceInWindow){//if there is still enough block memory
                    
                    if(i==sizeResult-1){ //making sure we add after checking
                      windowMemory.add(outer_tuple);

                    }
                    
                    
                  }else{
                    //Write into the temporary heapfile
                    System.out.println("There isn't enough space");

                    byte [] outer_bytes=outer_tuple.returnTupleByteArray();
                    try {
                      tempHeap.insertRecord(outer_bytes);
                    } catch (Exception e) {
                      //TODO: handle exception
                      e.printStackTrace();
                    }
                    
                  }
                }
              }
            }
      }
    }catch(Exception e) {
      e.printStackTrace();
    }
      done = true;
      }while(true);

      finalOutput.addAll(windowMemory);
    
      return windowMemory;
    }




//When the temporary heapfile is not empty
public ArrayList<Tuple> blockSkyline (String relName)throws IOException,NestedLoopException{
  
  // ArrayList<Tuple> result = new ArrayList<Tuple>();
    RID rid = new RID();
    

    
      do{
        
        if(done2){
          break;
        }
        System.out.println("HUrray, I m hereeee.........................");

  
    
    // creating a heapfile from the relationName
    // try {
    //   tempHeap = new Heapfile(relName);
    // }
    // catch(Exception e) {
    //   throw new NestedLoopException(e, "Create new heapfile failed.");
    // }
    //opening the scan
    
    try{
      outer = tempHeap.openScan();
    }catch(Exception e){
      throw new NestedLoopException(e, "openScan failed");
    }
    


    Tuple outerGetNext =null;
    try{
      outerGetNext = outer.getNext(rid);
    }catch(Exception e){
      throw new NestedLoopException(e, "Cannot get next tuple");
    }
    if((outer_tuple=outerGetNext)== null){
      //DOne scanning the input file
      done2 = true;
      break;
    }

    //  = outer.getNext(rid));
    boolean windowDominate=false;
    boolean outerDominate = false;
  
      if(windowMemory.isEmpty()){
        windowMemory.add(outer_tuple);

      }else{
        int sizeResult = windowMemory.size();
        for(int i = 0; i<sizeResult ; i++){
          Tuple windowTuple = windowMemory.get(i);
          // innerTuple, _in1, currentOuter, _in1, in1_len, t1_str_sizes_cls, pref_list_cls, pref_list_length_cls
          //Checking domination
          try{
            windowDominate = TupleUtils.Dominates(windowTuple,_in1,outer_tuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);// window dominate outer
          }
            catch(Exception e) {
              e.printStackTrace();
            }
          try{outerDominate = TupleUtils.Dominates(outer_tuple,_in1,windowTuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);//outer dominate window
          }catch(Exception e) {
            e.printStackTrace();
          }

          if(windowDominate){
            try{
              tempHeap.deleteRecord(rid);//delete record from the heap file
            }catch(Exception e){
              e.printStackTrace();
            }
          }

          if(outerDominate){

            if(windowMemory.size()>=maxRecordSize){
              //remove reocrd from window and set spaceWindow to false 
              windowMemory.remove(windowTuple);
              spaceInWindow = false;
            }else{
              windowMemory.remove(windowTuple);
            }
            
            if(spaceInWindow){//if there is still enough block memory
              windowMemory.add(outer_tuple);
            }else{
              //Write into the temporary heapfile
              byte [] outer_bytes=outer_tuple.getTupleByteArray();
              // tempHeap.insertRecord(outer_bytes);
            }
          }
        
        
        //if outer_tuple neither dominate nor is dominated
        if(!windowDominate && !outerDominate){


          if(windowMemory.size()>=maxRecordSize){
            spaceInWindow= false;
          }

          if(spaceInWindow){//if there is still enough block memory
            windowMemory.add(outer_tuple);
          }else{
            //Write into the temporary heapfile
            byte [] outer_bytes=outer_tuple.getTupleByteArray();
            // tempHeap.insertRecord(outer_bytes);
          }
        }
    }
  }

  }while(true);
  // finalOutputIter.add();
  finalOutput.addAll(windowMemory);

  return windowMemory;

  }

 
  /**
   * implement the abstract method close() from super class Iterator
   *to finish cleaning up
   *@exception IOException I/O error from lower layers
   *@exception JoinsException join error from lower layers
   *@exception IndexException index access error 
   */
  public void close() throws JoinsException, IOException,IndexException 
    {
      if (!closeFlag) {
	
	try {
	  am1_iter.close();
	}catch (Exception e) {
	  throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }
}