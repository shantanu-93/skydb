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
          throw new NestedLoopException(e, "Create new heapfile failed.");
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
        countTempR = tempHeap.getRecCnt();
        
        // System.out.println("The count in the heap file is: "+tempHeap.getRecCnt());
       }catch (Exception e) {
         e.printStackTrace();
       }
    }
    
    // System.out.println("The count in the heap file is: "+tempScan.getTotalNumberRecords());

    if(tempHeap == null || countTempR<=0){

      try{
        blockSkyline(hf, tempHeap);
        // System.out.println(tempHeap.getRecCnt());
        // blockSkyline("temporary.in");
        
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
    if(tempHeap != null){
      try {
        countTempR = tempHeap.getRecCnt();
        // System.out.println(countTempR);
      } catch (Exception e) {
        //TODO: handle exception
        e.printStackTrace();
      }
    }
    // System.out.println("The count in the heap file is: "+ tempScan.getTotalNumberRecords());
    //empty the window block
    windowMemory.clear();
    System.out.println("====================this is the new point======================"+countTempR);

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
          // System.out.println(countTempR);
        } catch (Exception e) {
          //TODO: handle exception
          e.printStackTrace();
        }
      }
      System.out.println("====================this is the new point======================"+countTempR);


      windowMemory.clear();
      // System.out.println(countTempR);
    
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
  
    public ArrayList<Tuple> blockSkyline(Heapfile inputHeap, Heapfile windowHeap) throws IOException,NestedLoopException{
      // ArrayList<Tuple> result = new ArrayList<Tuple>();
      RID rid = new RID();
      iterationNumber++;
      done = false; //reinitialize for the subsequent recursions
      spaceInWindow = true;
      
      
      try {
        System.out.println("HH Inmput before"+ inputHeap.getRecCnt());
        
        // while(windowHeap.getRecCnt()>1){
        //   Scan s = new Scan(windowHeap);
        //   s.getNext(tempRid);
        //   windowHeap.deleteRecord(tempRid);
        //   System.out.println("removed windowHeap items"+windowHeap.getRecCnt());
        //   s=null;
        // }
        if(windowHeap.getRecCnt()>1){
          windowHeap.deleteFile();
          windowHeap=new Heapfile("temporary.in");
        }
        System.out.println ("Window Heap size: "+windowHeap.getRecCnt());
        RID tempRid = new RID();

        System.out.println("HH Inmput"+ inputHeap.getRecCnt());
      } catch (Exception e) {
        //TODO: handle exception
      }
      
      do{
        System.out.println("when opening scan"+done);
        // RID rid = new RID();
        if(done){
          break;
        }
        
        try{
          
          outer = inputHeap.openScan();
          System.out.println("Input counts"+inputHeap.getRecCnt());


        }catch(Exception e){
          throw new NestedLoopException(e, "openScan failed");
        }
        
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

        
        while(outer!=null &&(outer_tuple=outer.getNext(rid))!=null){
          

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
                boolean inserted = false;
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
                      inputHeap.deleteRecord(rid);//delete record from the heap file
                    } catch (Exception e) {
                      e.printStackTrace();
                    }
                    
                  }

                  if(outerDominate){

                    if(windowMemory.size()>=maxRecordSize){
                      //remove reocrd from window and set spaceWindow to false
                      windowMemory.remove(windowTuple);
                      spaceInWindow = false;
                    }if(windowMemory.size()<maxRecordSize){
                      windowMemory.remove(windowTuple);
                    }
                    
                    if(spaceInWindow){//if there is still enough block memory
                      
                      if(!windowMemory.contains(outer_tuple)){
                        windowMemory.add(outer_tuple);
                        
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

                  if(spaceInWindow){//if there is still enough block memory
                    
                    if(i==sizeResult-1){ //making sure we add after checking
                      // System.out.println("Enough space");
                      windowMemory.add(outer_tuple);

                    }
                    
                    
                  }else{
                    //Write into the temporary heapfile
                    if(!inserted){
                      byte [] outer_bytes=outer_tuple.returnTupleByteArray();
                      try {
                        windowHeap.insertRecord(outer_bytes);
                        inserted=true;
                        // System.out.println(tempHeap.getRecCnt());
                        

                      } catch (Exception e) {
                        //TODO: handle exception
                        e.printStackTrace();
                      }
                  }
                    
                  }
                }
              }
              
              if(iterationNumber>1){//Avoiding to delete the actual input heap files


                if(inputHeap.getRecCnt()>1){//deleting the temporary heapfile
                  inputHeap.deleteRecord(rid);
                  // System.out.println("Hurray.......... "+inputHeap.getRecCnt());
                  // System.out.println("input size: "+hf.getRecCnt());
                  // System.out.println("windowheap size: "+windowHeap.getRecCnt());
                  // System.out.println(outer);
                }
                else if(inputHeap.getRecCnt()==1){
                  done=true;
                  break;
                }
            }
            }
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
        
        // this.hf = windowHeap;
      
      
        
        System.out.println("Hf at the end "+hf.getRecCnt());
        System.out.println("Outside");
        System.out.println("Window at the end "+tempHeap.getRecCnt());
      } catch (Exception e) {
        //TODO: handle exception
      }
      System.out.println("WIndows Memory"+windowMemory.size());
      finalOutput.addAll(windowMemory);
    
      return windowMemory;
    }




//When the temporary heapfile is not empty
// public ArrayList<Tuple> blockSkyline (String relName)throws IOException,NestedLoopException{
//   spaceInWindow= true;
  
  
//   // try {
//     // hf.deleteFile();
//     // System.out.println("hf before rec"+hf.getRecCnt());
//     // hf = null;
//     // hf=tempHeap;
//     // System.out.println("hf  after rec"+hf.getRecCnt());
//     // // tempHeap.deleteFile();
//     // tempHeap = null;
//     // // System.out.println("temo before rec"+tempHeap.getRecCnt());
//     // tempHeap = new Heapfile(relName);
//     // System.out.println("hf  after2 rec"+tempHeap.getRecCnt());



//   // } catch (Exception e) {
//   //   //TODO: handle exception

//   // }
  
  
//   // ArrayList<Tuple> result = new ArrayList<Tuple>();
//     RID rid =new RID();
//       do{
//         // rid = 
//         if(done2){
//           break;
//         }
//     // creating a heapfile from the relationName
//     // try {
//     //   tempHeap = new Heapfile(relName);
//     // }
//     // catch(Exception e) {
//     //   throw new NestedLoopException(e, "Create new heapfile failed.");
//     // }
//     //opening the scan
    
//     // if(tempHeap == null){
//     //   done2 = true;
//     //   break;
//     // }else{
//     try{
//       outer = tempHeap.openScan();
//     }catch(Exception e){
//       throw new NestedLoopException(e, "openScan failed");
//     }
//     // }
    
    
//     Tuple outerGetNext =null;
//     try{
//       outerGetNext = outer.getNext(rid);
//       System.out.println("outer "+ outer.getNext(rid));
//     }catch(Exception e){
//       throw new NestedLoopException(e, "Cannot get next tuple");
//     }
//     if((outer_tuple=outerGetNext)== null){
//       //DOne scanning the input file
//       done2 = true;
//       break;
//     }
//     else{
      
//       try {
//         // if(tempHeap.getRecCnt()>1){
        
//           tempHeap.deleteRecord(rid);
//           System.out.println("Records: "+tempHeap.getRecCnt());
//           System.out.println("Window size: "+windowMemory.size());
        
//       // }
      
//     }catch (Exception e) {
//         //TODO: handle exception
//         e.printStackTrace();
//       }
//     }

//     try{
//       outer_tuple.setHdr((short) 2, _in1, _t1_str_sizes);
//     }catch (Exception e) {
//       e.printStackTrace();
//     }

//     //  = outer.getNext(rid));
//     boolean windowDominate=false;
//     boolean outerDominate = false;
  
//       if(windowMemory.isEmpty()){
//         windowMemory.add(outer_tuple);

//       }else{
//         int sizeResult = windowMemory.size();
//         boolean inserted = false;
//         boolean deleted = false;

//         for(int i = 0; i<sizeResult ; i++){
//           Tuple windowTuple = windowMemory.get(i);
//           // innerTuple, _in1, currentOuter, _in1, in1_len, t1_str_sizes_cls, pref_list_cls, pref_list_length_cls
//           //Checking domination
//           try{
//             windowDominate = TupleUtils.Dominates(windowTuple,_in1,outer_tuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);// window dominate outer
//             // windowDominate = TupleUtils.Dominates(windowTuple,_in1,outer_tuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);// window dominate outer

//           }
//             catch(Exception e) {
//               e.printStackTrace();
//             }
//           try{outerDominate = TupleUtils.Dominates(outer_tuple,_in1,windowTuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);//outer dominate window
//           }catch(Exception e) {
//             e.printStackTrace();
//           }

//           if(windowDominate){
//             try{
//               // if(tempHeap.getRecCnt()>0){
//               //   tempHeap.deleteRecord(rid);//delete record from the heap file
//               // }
//               if(!deleted){
//                 tempHeap.deleteRecord(rid);
//                 deleted = true;
//               }
              
//             }catch(Exception e){
//               e.printStackTrace();
//             }
//           }

//           if(outerDominate){
//             if(windowMemory.size()>=maxRecordSize){
//               //remove reocrd from window and set spaceWindow to false 
//               windowMemory.remove(windowTuple);
//               spaceInWindow = false;
//             }else{
//               windowMemory.remove(windowTuple);
//             }
            
//             if(spaceInWindow){//if there is still enough block memory
//               windowMemory.add(outer_tuple);
//             }else{
//               //Write into the temporary heapfile
//               byte [] outer_bytes=outer_tuple.getTupleByteArray();

//               if(!inserted){
//                 try {
//                   tempHeap.insertRecord(outer_bytes);
//                   inserted = true;
//                 } catch (Exception e) {
//                   //TODO: handle exception
//                   e.printStackTrace();
//                 }
//               }
              
//             }
            
//           }
        
        
//         //if outer_tuple neither dominate nor is dominated
//         if(!windowDominate && !outerDominate){
          
//           if(windowMemory.size()>=maxRecordSize){
//             spaceInWindow= false;
//           }

//           if(spaceInWindow){//if there is still enough block memory
//             windowMemory.add(outer_tuple);
//             System.out.println("Hello world");
//             // try {
//             //  System.out.println(tempHeap.getRecCnt());

//             // } catch (Exception e) {
//             //   e.printStackTrace();
//             // }
            
//           }else{
//               //Write into the temporary heapfile
//               byte [] outer_bytes=outer_tuple.getTupleByteArray();
//               if(!inserted){
//                 try {
//                   tempHeap.insertRecord(outer_bytes);

//                 } catch (Exception e) {
//                   //TODO: handle exception
//                   e.printStackTrace();
//                 }
//               }
//             }
//         }

        
//     }
//     outer.closescan();
//     outer = null;
//   }

//   }while(true);
//   // finalOutputIter.add();
//   System.out.println("Window memory size: "+windowMemory.size());
//   finalOutput.addAll(windowMemory);
  
//   return windowMemory;
//   }

 
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