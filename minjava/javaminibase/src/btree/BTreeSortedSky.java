package btree;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

import java.io.IOException;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.TupleUtils;
import iterator.Iterator;

import iterator.RelSpec;

import iterator.SortFirstSky;
import readdriver.ReadDriver;

public class BTreeSortedSky implements GlobalConst {
	
	private AttrType[] attrType;
	private int attr_len;
	private short[] t1_str_sizes;
	private Iterator am1;
	private String relationName;
	private int[] pref_list;
	private int pref_list_length;
	private IndexFile index_file;
	private int n_pages;
	private static RelSpec rel = new RelSpec(RelSpec.outer);

	boolean status = OK;
	private static Tuple[] buffer_window;
	
	private Heapfile temp;
	
	// Constructor for BTreeSortedSky
	public BTreeSortedSky(AttrType[] attrType, int attr_len, short[] t1_str_sizes, Iterator am1, String relationName, int[] pref_list, int pref_list_length, IndexFile index_file,
			int n_pages) throws Exception {

		this.relationName = relationName;
		this.index_file = index_file;
		this.attrType = attrType;
		this.attr_len = attr_len;
		this.t1_str_sizes = t1_str_sizes;
		this.am1 = am1;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		this.n_pages = n_pages; 	
	}
	
	public void computeSkyline() throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {
		
		Heapfile hf = new Heapfile(relationName);
		String temp_heap_name = "tempheap.in";
		temp = new Heapfile(temp_heap_name);
		
		BTFileScan scan = ((BTreeFile) index_file).new_scan(null, null);
		KeyDataEntry entry;
		RID rid;
		
		Tuple t = getTuple();
			
		buffer_window = new Tuple[(MINIBASE_PAGESIZE / t.size()) * n_pages];
		
		System.out.println("Size of Buffer Window: " + (MINIBASE_PAGESIZE / t.size()) * n_pages);
		
		//Getting the first tuple
	    	entry = scan.get_next();
		
		// For counting window size
		int count = 0;
		
		// Total read tuples
		int total = 0;
		
		// Enter the tuples in temp heap and buffer window
		while (entry != null && count < buffer_window.length) {
			total++;

			rid = ((LeafData) entry.data).getData();
			Tuple temp_tuple = hf.getRecord(rid);
			
			temp_tuple.setHdr((short) 5, attrType, t1_str_sizes); 
			buffer_window[count++] = temp_tuple;
			temp_tuple.print(attrType);
			
			temp.insertRecord(temp_tuple.returnTupleByteArray());
		    entry = scan.get_next();
		}
	    
		int temp_file_size = 0;
		
		// Iterate through the heap file and find dominating tuples (skyline candidates)
        while (entry != null) {
            boolean check = false;
            total++;
            Tuple heap_tuple = getTuple();
            
            rid = ((LeafData) entry.data).getData();
            heap_tuple.tupleCopy(hf.getRecord(rid));
		
			for(int i = 0; i < buffer_window.length; i++){
				// Replace tuple from heap file with the tuple in window as it is dominated
				if (TupleUtils.Dominates(buffer_window[i] , attrType, heap_tuple, attrType, (short) attr_len, t1_str_sizes, pref_list, pref_list_length)) {
					check = true;
					// System.out.println("Heap tuple");
					// heap_tuple.print(attrType);
					// System.out.println("Dominated by ");
					// buffer_window[i].print(attrType);
					buffer_window[i] = heap_tuple;
					break;
				} 
			}

			// Add the heap file tuple in temp heap file as it is not dominated by any tuple in the window
			if(!check){

				temp_file_size++;

				try {
					heap_tuple.setHdr((short) 5, attrType, t1_str_sizes); 
					rid = temp.insertRecord(heap_tuple.returnTupleByteArray());
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			} 
			entry = scan.get_next();
        }
		
		scan.DestroyBTreeFileScan();
		// System.out.println("Temp File objects ");
		
		// RID tempRid = new RID();
		// Scan scanTempHF = new Scan(temp);
		// while (true) {
		// 	Tuple tempHFTuple = scanTempHF.getNext(tempRid);
		// 	if (tempHFTuple == null)
		// 		break;
		// 	tempHFTuple.setHdr((short) 5, attrType, t1_str_sizes); 
		// 	tempHFTuple.print(attrType);
		// }
		
		/// scanTempHF.closescan(); 

		System.out.println("Temp file records:" + temp.getRecCnt());
        System.out.println("Total data: " + total);
        System.out.println("Buffer Size: " + buffer_window.length);
		System.out.println("Total data put in temp file: " + temp_file_size);

		// SystemDefs.JavabaseBM.flushPages();
		ReadDriver.runSortFirstSky(temp_heap_name);
		// SystemDefs.JavabaseBM.flushPages();

		temp.deleteFile();
		hf.deleteFile();
		// Heapfile heap = new Heapfile(ReadDriver.heapFile);
		// heap.deleteFile();
	}
	
	private Tuple getTuple() throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}

}
