package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;

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
  String tempHFName;
  Heapfile tempHF = null;
  String tempHF1Name;
  Heapfile tempHF1 = null;
  Scan finalRes;


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
			   ) throws IOException, NestedLoopException, UnknowAttrType, TupleUtilsException, HFDiskMgrException, HFBufMgrException, HFException, InvalidTupleSizeException, InvalidTypeException, SpaceNotAvailableException, InvalidSlotNumberException, FileAlreadyDeletedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
      
      _in1 = new AttrType[in1.length];
      System.arraycopy(in1,0,_in1,0,in1.length);
      in1_len = (short)len_in1;

      inner_tuple = new Tuple();
      Jtuple = new Tuple();
      
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

      tempHFName = getRandomName();
      tempHF1Name = getRandomName();

        Heapfile mainFile;
        mainFile = new Heapfile(relationName);
        Scan tempScan = new Scan(mainFile);

        tempHF = new Heapfile(tempHFName);

        while (true) {
          Tuple tempTuple;

          RID tempRid = new RID();
          tempTuple = tempScan.getNext(tempRid);

            if (tempTuple == null) {
                break;
            }

          byte [] tempBytes = tempTuple.returnTupleByteArray();
          tempHF.insertRecord(tempBytes);
        }

        int count = 0;

        boolean changeFile = false;

        Scan outer = new Scan(mainFile);
        while (true) {

            Scan inner = new Scan(tempHF);

            Tuple outerTuple;

            RID tempRid = new RID();
            outerTuple = outer.getNext(tempRid);

            if (outerTuple == null) {
                break;
            }

            outerTuple.setHdr(in1_len, _in1, _t1_str_sizes);

            int innerCount = 0;

            while (true) {

                Tuple innerTuple;

                RID tempRid1 = new RID();
                innerTuple = inner.getNext(tempRid1);

                if (innerTuple == null) {
                    inner.closescan();
                    break;
                }

                innerTuple.setHdr(in1_len, _in1, _t1_str_sizes);

                if (SystemDefs.JavabaseBM.getNumBuffers() - SystemDefs.JavabaseBM.getNumUnpinnedBuffers() >= n_buf_pgs) {
                    SystemDefs.JavabaseBM.flushPages();
                }

                if (!TupleUtils.Dominates(outerTuple, _in1, innerTuple, _in1, in1_len, _t1_str_sizes, _pref_list, _pref_list_length)) {
                    if (tempHF1 == null) {
                        if (changeFile) {
                            tempHF1 = new Heapfile(tempHFName);
                        } else {
                            tempHF1 = new Heapfile(tempHF1Name);
                        }
                    }
                    innerCount++;
                    byte [] innerBytes = innerTuple.returnTupleByteArray();
                    tempHF1.insertRecord(innerBytes);
                }

//                System.out.println("innerCount: "+ innerCount);

            }
            System.out.println("Count after outer pass "+ count + " : " + tempHF1.getRecCnt());
//            System.out.println(count);
            count++;
            SystemDefs.JavabaseBM.flushPages();
            tempHF.deleteFile();
            tempHF = tempHF1;
            tempHF1 = null;

            changeFile = !changeFile;
        }
      finalRes = new Scan(tempHF);
        outer.closescan();
    }


       public Tuple get_next() throws InvalidTupleSizeException, IOException, InvalidTypeException {
        if (finalRes == null) {
            return null;
        } else {
            RID tempRid = new RID();
            Tuple res = finalRes.getNext(tempRid);
            if (res != null) {
                res.setHdr(in1_len, _in1, _t1_str_sizes);
                return res;
            }
        }
        return null;
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
	    finalRes.closescan();
          try {
              SystemDefs.JavabaseBM.flushPages();
          } catch (PageNotFoundException e) {
              e.printStackTrace();
          } catch (BufMgrException e) {
              e.printStackTrace();
          } catch (HashOperationException e) {
              e.printStackTrace();
          } catch (PagePinnedException e) {
              e.printStackTrace();
          }
          if (tempHF != null) {
              try {
                  tempHF.deleteFile();
              } catch (Exception e) {
              }
          }
	try {

	}catch (Exception e) {
	  throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }

    protected String getRandomName() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }
}
