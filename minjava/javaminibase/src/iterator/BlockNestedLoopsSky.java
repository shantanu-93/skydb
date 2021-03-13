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
  Heapfile tempHf;
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
  int iterationNumber;
  Heapfile copyHp;


  
  
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
      iterationNumber = 0;
      outer_tuple = new Tuple();
      //Creating an output iterator
      finalOutput = new ArrayList<Tuple>();
      // tempHFName = getRandomName();

      
      //Getting the maximum number of records on one page. 
      id = new RID();
      try {
	      hf = new Heapfile(relationName);
        tempHf = new Heapfile("temporaryInput.in");
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
      

      // maxRecordSize = sc.getNumberOfRecordsPerOnePage()* n_buff_pgs;
      maxRecordSize = sc.getNumberOfRecordsPerOnePage()* n_buff_pgs;

      sc = null;
      hf = null;

      try {
        tempHeap = new Heapfile("temporary.in");
      } catch (Exception e) {
          throw new NestedLoopException(e, "Create new heapfile failed.");
      }
      
      try {
	      hf = new Heapfile(relationName);
      }
      catch(Exception e) {
	      throw new NestedLoopException(e, "Create new heapfile failed.");
      }



      //Copy hf into another 
//       try {
//         copyHp = new Heapfile(tempHFName);        outerHf = new Heapfile(relationName);
//         Scan tempScan = new Scan(hf);

//         while (true) {
//             Tuple tempTuple;

//             RID tempRid = new RID();
//             tempTuple = tempScan.getNext(tempRid);

//             if (tempTuple == null) {
//                 break;
//             }

//             byte [] tempBytes = tempTuple.returnTupleByteArray();
//             copyHp.insertRecord(tempBytes);
//         }
//     }
//     catch(Exception e) {
// throw new NestedLoopException(e, "Create new heapfile failed.");
//     }


      
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
        countTempR = tempHeap.getRecCnt();
        
       }catch (Exception e) {
         e.printStackTrace();
       }
    }
    
    if((tempHeap == null || countTempR<=0)&& iterationNumber<1){

      try{
        blockSkyline(hf, tempHeap);
        
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
    if(tempHeap != null){
      try {
        countTempR = tempHeap.getRecCnt();
      } catch (Exception e) {
        //TODO: handle exception
        e.printStackTrace();
      }
    }
    //empty the window block
    windowMemory.clear();

    //deal with the temporary files
     if(tempHeap != null && countTempR>0){
      // if temporary file is not empty 
      // call the function again
      try{
        // blockSkyline("temporary.in");

        blockSkyline(tempHf, tempHeap);
        
        // windowIterator = windowMemory.iterator();

      }catch(Exception e) {
        e.printStackTrace();
      }
      if(tempHeap != null){
        try {
          countTempR = tempHeap.getRecCnt();
        } catch (Exception e) {
          //TODO: handle exception
          e.printStackTrace();
        }
      }

      windowMemory.clear();
    
      while(countTempR>0){
        // performSkyline();
        try {
          blockSkyline(tempHf, tempHeap);
          windowMemory.clear();
        } catch (Exception e) {
          //TODO: handle exception
        }
        if(tempHeap != null){
          try {
            countTempR = tempHeap.getRecCnt();
          } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
          }
        }
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
  
    public ArrayList<Tuple> blockSkyline(Heapfile inputHeap, Heapfile windowHeap) throws IOException,NestedLoopException{
      RID rid = new RID();
      RID copyRid = new RID();
      iterationNumber++;
      done = false; //reinitialize for the subsequent recursions
      spaceInWindow = true;
      // copyScan = null;


      
      
      try {
        
        if(windowHeap.getRecCnt()>1){
          windowHeap.hasNotBeenDeleted();
          windowHeap.deleteFile();
          windowHeap=new Heapfile("temporary.in");
        }
        RID tempRid = new RID();

      } catch (Exception e) {
        //TODO: handle exception
        
      }


      
      do{
        // RID rid = new RID();
        if(done){
          break;
        }
        
        try{
          outer = inputHeap.openScan();
          // copyScan = copyHp.openScan();

        }catch(Exception e){
          throw new NestedLoopException(e, "openScan failed");
        }
        
        Tuple outerGetNext =null;
        Tuple outerTemp = null;
        
        try{

        
        boolean isWindowFull = false;
        while((outer_tuple=outer.getNext(rid))!=null){
          // outerTemp = copyScan.getNext(copyRid);
          // if(outerTemp==null){
          //   break;
          // }
          

          try{
            outer_tuple.setHdr(in1_len, _in1, _t1_str_sizes);
            // outerTemp.setHdr(in1_len, _in1, _t1_str_sizes);
          }catch (Exception e) {
            e.printStackTrace();
          }
      
            boolean windowDominate=false;
            boolean outerDominate = false;
          
              if(windowMemory.isEmpty()){          
                windowMemory.add(outer_tuple);
              }else{
                
                java.util.Iterator iterWindow = windowMemory.iterator();
                int sizeResult = windowMemory.size();
                ArrayList<Tuple> temporaryWindow = new ArrayList<Tuple>();
                boolean inserted = false;
                // for(int i = 0; i<sizeResult ; i++){
                int i = 0;

                while(iterWindow.hasNext()){
                  
                  // System.out.println("Memory size: "+sizeResult+" i= "+i);
                  Tuple windowTuple = (Tuple)iterWindow.next();
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

                  if(windowDominate){
                    try {
                      // inputHeap.deleteRecord(rid);//delete record from the heap file
                      // rid = new RID();
                      break;
                    } catch (Exception e) {
                      e.printStackTrace();
                    }
                    
                  }

                  if(outerDominate){

                    if(windowMemory.size()>=maxRecordSize){
                      //remove reocrd from window and set spaceWindow to false
                      iterWindow.remove();
                     
                      isWindowFull = true;
                      spaceInWindow = false;
                    }else if(windowMemory.size()<maxRecordSize){
                      iterWindow.remove();
                      
                    }
                    
                    if(spaceInWindow && !isWindowFull){//if there is still enough block memory
                      
                      if(!windowMemory.contains(outer_tuple) && !temporaryWindow.contains(outer_tuple)){
                        temporaryWindow.add(outer_tuple);
                      }
     
                    }else{
                      //Write into the temporary heapfile
                      if(!inserted){
                        byte [] outer_bytes=outer_tuple.returnTupleByteArray();
                        try {
                          windowHeap.insertRecord(outer_bytes);
                          inserted = true;
                        } catch (Exception e) {
                          //TODO: handle exception
                          e.printStackTrace();
                        }
                    }
                      
                      
                    }
                  }
                
                //if outer_tuple neither dominate nor is dominated
                if(!windowDominate && !outerDominate){


                  if(windowMemory.size()>=maxRecordSize){
                    spaceInWindow= false;
                  }

                  if(spaceInWindow && !isWindowFull){//if there is still enough block memory
                    
                    if(i==sizeResult-1){ //making sure we add after checking
                      if(!windowMemory.contains(outer_tuple) && !temporaryWindow.contains(outer_tuple)){

                      temporaryWindow.add(outer_tuple);
                      // sizeResult++;
                      }

                    }
                    
                    
                  }else{
                    //Write into the temporary heapfile
                    if(!inserted){
                      byte [] outer_bytes=outer_tuple.returnTupleByteArray();
                      try {
                        windowHeap.insertRecord(outer_bytes);
                        inserted=true;                        

                      } catch (Exception e) {
                        //TODO: handle exception
                        e.printStackTrace();
                      }
                  }
                    
                  }
                }
                i++;
              }
              windowMemory.addAll(temporaryWindow);
               
            }
          SystemDefs.JavabaseBM.flushPages();

      }

      if(iterationNumber>1){//Avoiding to delete the actual input heap files
        inputHeap.hasNotBeenDeleted();
        inputHeap.deleteFile();
        inputHeap = new Heapfile("temporaryInput.in");
    }
      
      
    }catch(Exception e) {
      e.printStackTrace();
    }
      done = true;
      }while(true);

      // this.hf = null;
      try {
        RID tempRid = new RID();
        Scan s = new Scan(tempHeap);

        Tuple tpl = null;
        while((tpl = s.getNext(rid))!=null){
          byte [] bytes=tpl.returnTupleByteArray();
          this.tempHf.insertRecord(bytes);
        }
        
        
      } catch (Exception e) {
        //TODO: handle exception
      }
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
	}catch (Exception e) {
	  throw new JoinsException(e, "BlockNestedLoopsSky.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }
}