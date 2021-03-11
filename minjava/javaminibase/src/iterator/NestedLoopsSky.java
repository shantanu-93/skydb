package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.ArrayList;
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

public class NestedLoopsSky  extends Iterator 
{
  private AttrType      _in1[],  _in2[];
  private   short        in1_len;
//   private   Iterator  outer;
  private   short t2_str_sizescopy[];
  private   CondExpr OutputFilter[];
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean        done,         // Is the join complete
    get_from_outer;                 // if TRUE, a tuple is got from outer
  private   Tuple     outer_tuple, inner_tuple;
  private   Tuple     Jtuple;           // Joined tuple
//   private   FldSpec   perm_mat[];
//   private   int        nOutFlds;
  private   Heapfile  hf;
  private Heapfile outerHf;
  private   Scan      inner;
  private Scan outer;
//   private ArrayList<Tuple> sm ;
  ArrayList<Tuple> finalOutput;
  java.util.Iterator finalOutputIter;
  int[] _pref_list;
  int _pref_list_length;
  short[] _t1_str_sizes;
  String relationName;
  Iterator am1_iter; 
  ArrayList<Tuple> inputList;

  

  
  
  /**constructor
   *Initialize the two relations which are joined, including relation type,
   *@param in1  Array containing field types of R.
   *@param len_in1  # of columns in R.
   *@param t1_str_sizes shows the length of the string fields.
   *@param in2  Array containing field types of S
   *@param len_in2  # of columns in S
   *@param  t2_str_sizes shows the length of the string fields.
   *@param amt_of_mem  IN PAGES
   *@param am1  access method for left i/p to join
   *@param relationName  access hfapfile for right i/p to join
   *@param outFilter   select expressions
   *@param rightFilter reference to filter applied on right i/p
   *@param proj_list shows what input fields go where in the output tuple
   *@param n_out_flds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
   */
  public NestedLoopsSky( 
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            Iterator am1,
            String relName,
            int[] pref_list,
            int pref_list_length,
            int n_pages   
			   ) throws IOException,NestedLoopException
    {
      
      _in1 = new AttrType[in1.length];
      System.arraycopy(in1,0,_in1,0,in1.length);
      in1_len = (short)len_in1;
      
    //   outer = am1;
    //   t2_str_sizescopy =  t2_str_sizes;
      inner_tuple = new Tuple();
      Jtuple = new Tuple();
    //   OutputFilter = outFilter;
    //   RightFilter  = rightFilter;
      
      n_buf_pgs    = n_pages;
      inner = null;
      done  = false;
      get_from_outer = true;
      _pref_list = pref_list;
      _pref_list_length = pref_list_length;
      _t1_str_sizes = t1_str_sizes;
      relationName = (String)relName;
      am1_iter = am1;
      finalOutput = new ArrayList<Tuple>();
      inputList = new ArrayList<Tuple>();


      
    //   AttrType[] Jtypes = new AttrType[n_out_flds];
    //   short[]    t_size;
      
    //   perm_mat = proj_list;
    //   nOutFlds = n_out_flds;
    //   try {
	// // t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
	// // 				   in1, len_in1, in1, len_in1,
	// // 				   t1_str_sizes, t1_str_sizes,
	// // 				   proj_list, nOutFlds);
    //   }catch (TupleUtilsException e){
	// throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
    //   }
      
      
      
      try {
	  hf = new Heapfile(relationName);
      outerHf = new Heapfile(relationName);
	  
      }
      catch(Exception e) {
	throw new NestedLoopException(e, "Create new heapfile failed.");
      }
    }
  

    public Tuple performSkyline() throws IOException{

        // try {
        //     RID rid = new RID();
        //     Tuple result = new Tuple();
        //     Scan rScan = new Scan(hf);
        //     while((result = rScan.getNext(rid))!= null){
        //         finalOutput.add(result);

        //     }
            
        // } catch (Exception e) {
        //     //TODO: handle exception
        //     e.printStackTrace();
        // }
        
          // call the function again
          try{    
            skyLine();
            
            // windowIterator = windowMemory.iterator();
    
          }catch(Exception e) {
            e.printStackTrace();
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
  public void skyLine()
    throws IOException,
	   JoinsException ,
	   IndexException,
	   InvalidTupleSizeException,
	   InvalidTypeException, 
	   PageNotReadException,
	   TupleUtilsException, 
	   PredEvalException,
	   SortException,
	   LowMemException,
	   UnknowAttrType,
	   UnknownKeyTypeException,
	   Exception
    {
      // This is a DUMBEST form of a join, not making use of any key information...
    // m = new ArrayList<Tuple>();
    RID outerRid = new RID();
    try {
        outer = outerHf.openScan();
        Tuple tpl = new Tuple();

        while ((tpl = outer.getNext(outerRid)) != null){
            tpl.setHdr((short)in1_len, _in1,_t1_str_sizes);
            inputList.add(tpl);
        }
        // System.out.println(inputList.size());
    } catch (Exception e) {
        //TODO: handle exception
        e.printStackTrace();
    }

    int inputSize = inputList.size();
    for (int i =0; i<inputSize; i++){
        for(int j=0; j<inputSize; j++){
            boolean outerDominate = false;
            // innerDominate = TupleUtils.Dominates(inner_tuple,_in1,outer_tuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);
            outerDominate = TupleUtils.Dominates(inputList.get(i),_in1,inputList.get(j), _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length);
            if(outerDominate){
                inputList.remove(inputList.get(j));
            }
            
        }
    }

    finalOutput = inputList;

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