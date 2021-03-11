package heap;

/** JAVA */
/**
 * Scan.java-  class Scan
 *
 *
 *
 */

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import java.util.ArrayList;


/**	
 * A Scan object is created ONLY through the function openScan
 * of a HeapFile. It supports the getNext interface which will
 * simply retrieve the next record in the heapfile.
 *
 * An object of type scan will always have pinned one directory page
 * of the heapfile.
 */
public class Scan implements GlobalConst{
 
    /**
     * Note that one record in our way-cool HeapFile implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The heapfile we are using. */
    private Heapfile  _hf;

    /** PageId of current directory page (which is itself an HFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private RID datapageRid = new RID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private HFPage datapage = new HFPage();

    /** record ID of the current record (from the current data page) */
    private RID userrid = new RID();

    /** Status of next user status */
    private boolean nextUserStatus;

    private boolean isPageLeftBlockScan;

    private RID nextPageBlockRid;

    private int numRecPerPage;

    private boolean doneCountFirstPage;

    private int totalRecords;

    /** The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
  public Scan(Heapfile hf) 
    throws InvalidTupleSizeException,
	   IOException
  {
	init(hf);
  }


  
  /** Retrieve the next record in a sequential scan
   *
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   *
   * @param rid Record ID of the record
   * @return the Tuple of the retrieved record.
   */
  public Tuple getNext(RID rid) 
    throws InvalidTupleSizeException,
	   IOException
  {

    Tuple recptrtuple = null;
    
    if (nextUserStatus != true) {
        nextDataPage();

    }

    if (datapage == null)
      return null;
    
    if(userrid == null)
      return null;

    rid.pageNo.pid = userrid.pageNo.pid;
    rid.slotNo = userrid.slotNo;
         
    try {
      recptrtuple = datapage.getRecord(rid);

    }
    
    catch (Exception e) {
  //    System.err.println("SCAN: Error in Scan" + e);
      e.printStackTrace();
    }   
    
    userrid = datapage.nextRecord(rid);
    if(userrid == null) nextUserStatus = false;
    else nextUserStatus = true;
     
    return recptrtuple;
  }



  public Tuple getNextAndCountRecords(RID rid)
  throws InvalidTupleSizeException,
   IOException
{

  do{

    Tuple recptrtuple = null;

      if (nextUserStatus != true) {
          break;

      }

      if (datapage == null)
        return null;

      rid.pageNo.pid = userrid.pageNo.pid;
      rid.slotNo = userrid.slotNo;

      try {
        recptrtuple = datapage.getRecord(rid);
        numRecPerPage++;
      }

      catch (Exception e) {
    //    System.err.println("SCAN: Error in Scan" + e);
        e.printStackTrace();
      }

      userrid = datapage.nextRecord(rid);
      if(userrid == null) nextUserStatus = false;
      else nextUserStatus = true;
  }while(true);
    return null;

}

public Tuple CountAllFileRecords(RID rid)
throws InvalidTupleSizeException,
 IOException
{

do{

  Tuple recptrtuple = null;

    if (nextUserStatus != true) {
      nextDataPage();
    }

    if (datapage == null)
      return null;

    rid.pageNo.pid = userrid.pageNo.pid;
    rid.slotNo = userrid.slotNo;

    try {
      recptrtuple = datapage.getRecord(rid);
      totalRecords++;
    }

    catch (Exception e) {
  //    System.err.println("SCAN: Error in Scan" + e);
      e.printStackTrace();
    }

    userrid = datapage.nextRecord(rid);
    if(userrid == null) nextUserStatus = false;
    else nextUserStatus = true;
}while(true);
  // return null;

}

public int getNumberOfRecordsPerOnePage(){
  return numRecPerPage;
}

public int getTotalNumberRecords(){
  return totalRecords;
}


    /** Position the scan cursor to the record with the given rid.
     * 
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @param rid Record ID of the given record
     * @return 	true if successful, 
     *			false otherwise.
     */
  public boolean position(RID rid) 
    throws InvalidTupleSizeException,
	   IOException
  { 
    RID    nxtrid = new RID();
    boolean bst;

    bst = peekNext(nxtrid);

    if (nxtrid.equals(rid)==true) 
    	return true;

    // This is kind lame, but otherwise it will take all day.
    PageId pgid = new PageId();
    pgid.pid = rid.pageNo.pid;
 
    if (!datapageId.equals(pgid)) {

      // reset everything and start over from the beginning
      reset();
      
      bst =  firstDataPage();

      if (bst != true)
	return bst;
      
      while (!datapageId.equals(pgid)) {
	bst = nextDataPage();
	if (bst != true)
	  return bst;
      }
    }
    
    // Now we are on the correct page.
    
    try{
    	userrid = datapage.firstRecord();
	}
    catch (Exception e) {
      e.printStackTrace();
    }
	

    if (userrid == null)
      {
    	bst = false;
        return bst;
      }
    
    bst = peekNext(nxtrid);
    
    while ((bst == true) && (nxtrid != rid))
      bst = mvNext(nxtrid);
    
    return bst;
  }


    /** Do all the constructor work
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
    private void init(Heapfile hf) 
      throws InvalidTupleSizeException,
	     IOException
  {
	_hf = hf;

    	firstDataPage();
  }

    public Tuple getNextAndCountRecords(RID rid)
            throws InvalidTupleSizeException,
            IOException
    {

        do{

            Tuple recptrtuple = null;

            if (nextUserStatus != true) {
                break;

            }

            if (datapage == null)
                return null;

            rid.pageNo.pid = userrid.pageNo.pid;
            rid.slotNo = userrid.slotNo;

            try {
                recptrtuple = datapage.getRecord(rid);
                numRecPerPage++;
            }

            catch (Exception e) {
                //    System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }

            userrid = datapage.nextRecord(rid);
            if(userrid == null) nextUserStatus = false;
            else nextUserStatus = true;
        }while(true);
        return null;

    }



    /** Closes the Scan object */
    public void closescan()
    {
    	reset();
    }

    public int getNumberOfRecordsPerOnePage(){
        return numRecPerPage;
    }

    /** Reset everything and unpin all pages. */
    private void reset()
    { 

    if (datapage != null) {
    
    try{
      unpinPage(datapageId, false);
    }
    catch (Exception e){
      // 	System.err.println("SCAN: Error in Scan" + e);
      e.printStackTrace();
    }  
    }
    datapageId.pid = 0;
    datapage = null;

    if (dirpage != null) {
    
      try{
	unpinPage(dirpageId, false);
      }
      catch (Exception e){
	//     System.err.println("SCAN: Error in Scan: " + e);
	e.printStackTrace();
      }
    }
    dirpage = null;
 
    nextUserStatus = true;

  }
 
 
  /** Move to the first data page in the file. 
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   * @return true if successful
   *         false otherwise
   */
  private boolean firstDataPage() 
    throws InvalidTupleSizeException,
	   IOException
  {
    DataPageInfo dpinfo;
    Tuple        rectuple = null;
    Boolean      bst;

    /** copy data about first directory page */
 
    dirpageId.pid = _hf._firstDirPageId.pid;  
    nextUserStatus = true;

    /** get first directory page and pin it */
    	try {
	   dirpage  = new HFPage();
       	   pinPage(dirpageId, (Page) dirpage, false);	   
       }

    	catch (Exception e) {
    //    System.err.println("SCAN Error, try pinpage: " + e);
	e.printStackTrace();
	}
    
    /** now try to get a pointer to the first datapage */
	 datapageRid = dirpage.firstRecord();
	 
    	if (datapageRid != null) {
    /** there is a datapage record on the first directory page: */
	
	try {
          rectuple = dirpage.getRecord(datapageRid);
	}  
				
	catch (Exception e) {
	//	System.err.println("SCAN: Chain Error in Scan: " + e);
		e.printStackTrace();
	}		
      			    
    	dpinfo = new DataPageInfo(rectuple);
        datapageId.pid = dpinfo.pageId.pid;

    } else {

    /** the first directory page is the only one which can possibly remain
     * empty: therefore try to get the next directory page and
     * check it. The next one has to contain a datapage record, unless
     * the heapfile is empty:
     */
      PageId nextDirPageId = new PageId();
      
      nextDirPageId = dirpage.getNextPage();
      
      if (nextDirPageId.pid != INVALID_PAGE){
	
	try {
            unpinPage(dirpageId, false);
            dirpage = null;
	    }
	
	catch (Exception e) {
	//	System.err.println("SCAN: Error in 1stdatapage 1 " + e);
		e.printStackTrace();
	}
        	
	try {
	
           dirpage = new HFPage();
	    pinPage(nextDirPageId, (Page )dirpage, false);
	
	    }
	
	catch (Exception e) {
	//  System.err.println("SCAN: Error in 1stdatapage 2 " + e);
	  e.printStackTrace();
	}
	
	/** now try again to read a data record: */
	
	try {
	  datapageRid = dirpage.firstRecord();
	}
        
	catch (Exception e) {
	//  System.err.println("SCAN: Error in 1stdatapg 3 " + e);
	  e.printStackTrace();
	  datapageId.pid = INVALID_PAGE;
	}
       
	if(datapageRid != null) {
          
	  try {
	  
	    rectuple = dirpage.getRecord(datapageRid);
	  }
	  
	  catch (Exception e) {
	//    System.err.println("SCAN: Error getRecord 4: " + e);
	    e.printStackTrace();
	  }
	  
	  if (rectuple.getLength() != DataPageInfo.size)
	    return false;
	  
	  dpinfo = new DataPageInfo(rectuple);
	  datapageId.pid = dpinfo.pageId.pid;
	  
         } else {
	   // heapfile empty
           datapageId.pid = INVALID_PAGE;
         }
       }//end if01
       else {// heapfile empty
	datapageId.pid = INVALID_PAGE;
	}
}	
	
	datapage = null;

	try{
         nextDataPage();
	  }
	  
	catch (Exception e) {
	//  System.err.println("SCAN Error: 1st_next 0: " + e);
	  e.printStackTrace();
	}
	
      return true;
      
      /** ASSERTIONS:
       * - first directory page pinned
       * - this->dirpageId has Id of first directory page
       * - this->dirpage valid
       * - if heapfile empty:
       *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
       * - if heapfile nonempty:
       *    - this->datapage == NULL, this->datapageId, this->datapageRid valid
       *    - first datapage is not yet pinned
       */
    
  }
    

  /** Move to the next data page in the file and 
   * retrieve the next data page. 
   *
   * @return 		true if successful
   *			false if unsuccessful
   */
  private boolean nextDataPage() 
    throws InvalidTupleSizeException,
	   IOException
  {
    DataPageInfo dpinfo;
    
    boolean nextDataPageStatus;
    PageId nextDirPageId = new PageId();
    Tuple rectuple = null;

  // ASSERTIONS:
  // - this->dirpageId has Id of current directory page
  // - this->dirpage is valid and pinned
  // (1) if heapfile empty:
  //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
  // (2) if overall first record in heapfile:
  //    - this->datapage==NULL, but this->datapageId valid
  //    - this->datapageRid valid
  //    - current data page unpinned !!!
  // (3) if somewhere in heapfile
  //    - this->datapageId, this->datapage, this->datapageRid valid
  //    - current data page pinned
  // (4)- if the scan had already been done,
  //        dirpage = NULL;  datapageId = INVALID_PAGE
    
    if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
        return false;

    if (datapage == null) {
      if (datapageId.pid == INVALID_PAGE) {
	// heapfile is empty to begin with
	
	try{
	  unpinPage(dirpageId, false);
	  dirpage = null;
	}
	catch (Exception e){
	//  System.err.println("Scan: Chain Error: " + e);
	  e.printStackTrace();
	}
	
      } else {
	
	// pin first data page
	try {
	  datapage  = new HFPage();
	  pinPage(datapageId, (Page) datapage, false);
	}
	catch (Exception e){
	  e.printStackTrace();
	}
	
	try {
	  userrid = datapage.firstRecord();
	}
	catch (Exception e) {
	  e.printStackTrace();
	}
	
	return true;
        }
    }
  
  // ASSERTIONS:
  // - this->datapage, this->datapageId, this->datapageRid valid
  // - current datapage pinned

    // unpin the current datapage
    try{
      unpinPage(datapageId, false /* no dirty */);
        datapage = null;
    }
    catch (Exception e){
      
    }
          
    // read next datapagerecord from current directory page
    // dirpage is set to NULL at the end of scan. Hence
    
    if (dirpage == null) {
      return false;
    }
    
    datapageRid = dirpage.nextRecord(datapageRid);
    
    if (datapageRid == null) {
      nextDataPageStatus = false;
      // we have read all datapage records on the current directory page
      
      // get next directory page
      nextDirPageId = dirpage.getNextPage();
  
      // unpin the current directory page
      try {
	unpinPage(dirpageId, false /* not dirty */);
	dirpage = null;
	
	datapageId.pid = INVALID_PAGE;
      }
      
      catch (Exception e) {
	
      }
		    
      if (nextDirPageId.pid == INVALID_PAGE)
	return false;
      else {
	// ASSERTION:
	// - nextDirPageId has correct id of the page which is to get
	
	dirpageId = nextDirPageId;
	
 	try { 
	  dirpage  = new HFPage();
	  pinPage(dirpageId, (Page)dirpage, false);
	}
	
	catch (Exception e){
	  
	}
	
	if (dirpage == null)
	  return false;
	
    	try {
	  datapageRid = dirpage.firstRecord();
	  nextDataPageStatus = true;
	}
	catch (Exception e){
	  nextDataPageStatus = false;
	  return false;
	} 
      }
    }
    
    // ASSERTION:
    // - this->dirpageId, this->dirpage valid
    // - this->dirpage pinned
    // - the new datapage to be read is on dirpage
    // - this->datapageRid has the Rid of the next datapage to be read
    // - this->datapage, this->datapageId invalid
  
    // data page is not yet loaded: read its record from the directory page
   	try {
	  rectuple = dirpage.getRecord(datapageRid);
	}
	
	catch (Exception e) {
	  System.err.println("HeapFile: Error in Scan" + e);
	}
	
	if (rectuple.getLength() != DataPageInfo.size)
	  return false;
                        
	dpinfo = new DataPageInfo(rectuple);
	datapageId.pid = dpinfo.pageId.pid;
	
 	try {
	  datapage = new HFPage();
	  pinPage(dpinfo.pageId, (Page) datapage, false);
	}
	
	catch (Exception e) {
	  System.err.println("HeapFile: Error in Scan" + e);
	}
	
     
     // - directory page is pinned
     // - datapage is pinned
     // - this->dirpageId, this->dirpage correct
     // - this->datapageId, this->datapage, this->datapageRid correct

     userrid = datapage.firstRecord();
     
     if(userrid == null)
     {
       nextUserStatus = false;
       return false;
     }
  
     return true;
  }


  private boolean peekNext(RID rid) {
    
    rid.pageNo.pid = userrid.pageNo.pid;
    rid.slotNo = userrid.slotNo;
    return true;
    
  }


  /** Move to the next record in a sequential scan.
   * Also returns the RID of the (new) current record.
   */
  private boolean mvNext(RID rid) 
    throws InvalidTupleSizeException,
	   IOException
  {
    RID nextrid;
    boolean status;

    if (datapage == null)
        return false;

    	nextrid = datapage.nextRecord(rid);
	
	if( nextrid != null ){
	  userrid.pageNo.pid = nextrid.pageNo.pid;
	  userrid.slotNo = nextrid.slotNo;
	  return true;
	} else {
	  
	  status = nextDataPage();

	  if (status==true){
	    rid.pageNo.pid = userrid.pageNo.pid;
	    rid.slotNo = userrid.slotNo;
	  }
	
	}
	return true;
  }

    /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
    }

  } // end of unpinPage



    /** Retrieve the next records from n pages in a sequential scan
   *
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   *
   * @param rid Record ID of the record
   * @return the Tuple of the retrieved record.
   */
  public ArrayList<Tuple> getNextRecordFromNPages(RID rid, int n_pages)
    throws InvalidTupleSizeException,
	   IOException
  {
     isPageLeftBlockScan = true;
    ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    for(int i = 0; i<n_pages; i++){


      if (nextUserStatus != true) {
        nextDataPage();
      }
      while(nextUserStatus){
        Tuple recptrtuple = null;

          if (datapage == null)
            return null;

          rid.pageNo.pid = userrid.pageNo.pid;
          rid.slotNo = userrid.slotNo;

          try {
            recptrtuple = datapage.getRecord(rid);
            tuples.add(recptrtuple);
          }

          catch (Exception e) {
        //    System.err.println("SCAN: Error in Scan" + e);
            e.printStackTrace();
          }

          userrid = datapage.nextRecord(rid);
          if(userrid == null) nextUserStatus = false;
          else nextUserStatus = true;

      }

    }

    isPageLeftBlockScan = nextDataPage();
    nextPageBlockRid = datapage.nextRecord(rid);

    return tuples;

  }

  public RID getnextPageBlockRid(){
    return nextPageBlockRid;
  }



}
