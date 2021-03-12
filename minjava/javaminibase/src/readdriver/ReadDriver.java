package readdriver;


import java.io.*;
import java.util.*;
import java.lang.*;

import diskmgr.PCounter;
import heap.*;
import global.*;
import btree.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.SortFirstSky;
import iterator.BlockNestedLoopsSky;
import iterator.NestedLoopsSky;
import tests.TestDriver;

/** Note that in JAVA, methods can't be overridden to be more private.
 Therefore, the declaration of all private functions are now declared
 protected as opposed to the private type in C++.
 */

//watching point: RID rid, some of them may not have to be newed.

class Driver  extends TestDriver implements GlobalConst
{
    protected String dbpath;
    protected String logpath;

    private static RID   rid;
    private static Heapfile  f = null;
    private boolean status = OK;
    private static String _fileName = "data2.txt";
    private static int[] pref_attr_lst;
    private static int _n_pages;
    private static int col;
    private static final String heapFile= "hFile_100.in";
    private static AttrType[] attrType;
    private short[] attrSize;
	private static short tSize = 34;
    // create an iterator by open a file scan
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);


    public Driver(){
        super("main");
    }

    public boolean runTests () {
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        dbpath = "/tmp/main"+System.getProperty("user.name")+".minibase-db";
        logpath = "/tmp/main"+System.getProperty("user.name")+".minibase-log";

        SystemDefs sysdef = new SystemDefs(dbpath,50000, 40000,"Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");

        return _pass;
    }

    private void readMenu() {
        System.out.println("-------------------------- MENU ------------------");
        System.out.println("[1]   Read input data 2");
        System.out.println("[2]   Read input data 3");
        System.out.println("[3]   Set pref = [1]");
        System.out.println("[4]   Set pref = [1,3]");
        System.out.println("[5]   Set pref = [1,3,5]");
        System.out.println("[6]   Set pref = [1,2,3,4,5]");
        System.out.println("[7]   Set n_page = 5");
        System.out.println("[8]   Set n_page = 10");
        System.out.println("[9]   Set n_page = <your_wish>");
        System.out.println("[10]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[11]  Run Block Nested Loop on data with parameters ");
        System.out.println("[12]  Run Sort First Sky on data with parameters ");
        System.out.println("[13]  Run Btree Sky on data with parameters ");
        System.out.println("[14]  Run Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Hi, Enter your choice :");
    }
    
    private void readData(String fileName) throws IOException, InvalidTupleSizeException, InvalidTypeException {

        // Create the heap file object
        try {
            f = new Heapfile(heapFile);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers() ) {
            System.err.println ("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if ( status == OK ) {

            // Read data and construct tuples
            File file = new File(fileName + ".txt");
            Scanner sc = new Scanner(file);

            col= sc.nextInt();

            attrType = new AttrType[col];
            attrSize = new short[0];

            for(int i=0; i<attrType.length; i++){
                attrType[i] = new AttrType(AttrType.attrReal);
            }

            projlist = new FldSpec[col];

            for(int i=0; i<col; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            Tuple t = new Tuple();
            try {
                t.setHdr((short) col,attrType, attrSize);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = t.size();
            System.out.println("Size: "+size);
			tSize = size;

            t = new Tuple(size);
            try {
                t.setHdr((short) col, attrType, attrSize);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            while (sc.hasNextLine()) {
                // create a tuple of appropriate size

                double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                        .split("\\s+"))
                        .filter(s -> !s.isEmpty())
                        .toArray(String[]::new))
                        .mapToDouble(Double::parseDouble)
                        .toArray();

                for(int i=0; i<doubleArray.length; i++) {
                    try {
                        t.setFloFld(i+1, (float) doubleArray[i]);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

//                for (int i = 1; i <= t.noOfFlds(); i++) {
//                    try {
//                        System.out.print(t.getFloFld(i) + ", ");
//                    } catch (FieldNumberOutOfBoundException e) {
//                        e.printStackTrace();
//                    }
//                }
//                System.out.println();

                try {
                    rid = f.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                //System.out.println("RID: "+rid);
            }
            try {
                System.out.println("record count "+f.getRecCnt());
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            } catch (HFDiskMgrException e) {
                e.printStackTrace();
            } catch (HFBufMgrException e) {
                e.printStackTrace();
            }
        }
    }
    protected float floatKey(Tuple t, int[] pref_attr_lst, int pref_attr_lst_len) {
		float sum = 0.0f;
        int j = 0;
		for(int i = 0; i < t.noOfFlds(); i++) {
			if(pref_attr_lst[j] == i && j < pref_attr_lst_len) {
				try {
					sum += t.getFloFld(i-1);
				}
				catch (Exception e){
					status = FAIL;
					e.printStackTrace();
				}
                j++;
			}
		}
		return sum;
	}

    private IndexFile[] createIndex( String heapFile, int col, int[] pref_attr_lst, int pref_attr_lst_len) throws Exception {
 
	BTreeFile btf = null;
	Scan scan = null;
	try {
		f = new Heapfile(heapFile);
	    }
	catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	}	
	try {
      		scan = new Scan(f);
    	}
    	catch (Exception e) {
      		status = FAIL;
      		e.printStackTrace();
      		Runtime.getRuntime().exit(1);
    	}
	int keyType = AttrType.attrReal;
        int keySize = 4;
        AttrType [] Stypes = new AttrType[col];
	IndexFile[] index_file_list = new IndexFile[pref_attr_lst_len];
	BTreeFile[] btree_file_list = new BTreeFile[pref_attr_lst_len];
	for(int i = 0; i< pref_attr_lst_len; i++){
		String index_file = "AAA_" + Integer.toString(pref_attr_lst[i]);
    		try {
      			btf = new BTreeFile(index_file, keyType, keySize, 1/*delete*/);
			btree_file_list[i] = btf;
    		}
    		catch (Exception e) {
      			status = FAIL;
      			e.printStackTrace();
      			Runtime.getRuntime().exit(1);
    		}
		System.out.println("BTreeIndex " + index_file + " created successfully.\n");
		
	}
	rid = new RID();
    	String key = null;
    	Tuple temp = null;
	Tuple t = new Tuple(tSize);
	float fkey;
	KeyClass ffkey;	
	try {
      		temp = scan.getNext(rid);
    	}
    	catch (Exception e) {
      		status = FAIL;
      		e.printStackTrace();
    	}
	while ( temp != null) {
      		t.tupleCopy(temp);
      
      	
			fkey = floatKey(t, pref_attr_lst, pref_attr_lst_len);
            ffkey = new FloatKey(fkey);
      	
			int j =  0;
      		for (int i = 0; i < t.noOfFlds(); i ++){
			if(pref_attr_lst[j] == i && j < pref_attr_lst_len) {

      			try {
				BTreeFile btf1 = null;
				btf1 = btree_file_list[i];
				btf1.insert(ffkey, rid);
			        index_file_list[i] = btf1;	
      			}
      			catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
      			}
			j++;	
			}
		}

      		try {
			temp = scan.getNext(rid);
      		}
      		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
      		}
    	}
    
    // close the file scan
    	scan.closescan();
   return index_file_list; 
        }

    

    protected String testName () {
        return "Main Driver";
    }

    protected boolean runAllTests (){
        int choice=1;

        while(choice!=0) {
            readMenu();

            try{
                choice= GetStuff.getChoice();

                switch(choice) {

                    case 1:
                        readData("/Users/musabafzal/Desktop/cse510dbmsi/minjava/javaminibase/data/data2");
                        break;

                    case 2:
                        readData("/Users/musabafzal/Desktop/cse510dbmsi/minjava/javaminibase/data/data3");
                        break;

                    case 3:
                        pref_attr_lst = new int[]{1};
                        break;

                    case 4:
                        pref_attr_lst = new int[]{1,3};
                        break;

                    case 5:
                        pref_attr_lst = new int[]{1,3,5};
                        break;

                    case 6:
                        pref_attr_lst = new int[]{1,2,3,4,5};
                        break;

                    case 7:
                        _n_pages = 5;
                        break;

                    case 8:
                        _n_pages = 10;
                        break;

                    case 9:
                        System.out.println("Enter n_pages of your choice: ");
                        _n_pages = GetStuff.getChoice();
                        if(_n_pages<0)
                            break;
                        break;
					case 10:
                        // call nested loop sky
                        FileScan fscanNested = null;

                        try {
                            fscanNested = new FileScan(heapFile, attrType, attrSize, (short) attrType.length, attrType.length, projlist, null);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        PCounter.initialize();

                        NestedLoopsSky nested = null;
                        try {
                            nested = new NestedLoopsSky(attrType, attrType.length, attrSize, fscanNested, heapFile, pref_attr_lst, pref_attr_lst.length, _n_pages);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        int nestedSkycount = -1;
                        Tuple nestedSkyTuple = null;

                        System.out.println("\n -- Skyline candidates -- ");
                        do {
                            try {
                                if (nestedSkyTuple != null) {
                                    for (int i = 1; i <= nestedSkyTuple.noOfFlds(); i++) {
                                        System.out.print(nestedSkyTuple.getFloFld(i) + ", ");
                                    }
                                    System.out.println();
                                }
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }

                            nestedSkycount++;

                            try {
                                nestedSkyTuple = nested.get_next();
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }
                        } while (nestedSkyTuple != null);

                        System.out.println("Read statistics "+PCounter.rcounter);
                        System.out.println("Write statistics "+PCounter.wcounter);

                        System.out.println("\n Number of Skyline candidates: " + nestedSkycount);

                        try {
                            fscanNested.close();
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        try {
                            nested.close();
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }
                        break;

                    case 11:
                        // call block nested loop sky
                        FileScan fscanBlock = null;

                        try {
                            fscanBlock = new FileScan(heapFile, attrType, attrSize, (short) attrType.length, attrType.length, projlist, null);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        PCounter.initialize();

                        BlockNestedLoopsSky block = null;
                        try {
                            block = new BlockNestedLoopsSky(attrType, attrType.length, attrSize, fscanBlock, heapFile, pref_attr_lst, pref_attr_lst.length, _n_pages);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        int blockSkycount = -1;
                        Tuple blockSkyTuple = null;

                        System.out.println("\n -- Skyline candidates -- ");
                        do {
                            try {
                                if (blockSkyTuple != null) {
                                    for (int i = 1; i <= blockSkyTuple.noOfFlds(); i++) {
                                        System.out.print(blockSkyTuple.getFloFld(i) + ", ");
                                    }
                                    System.out.println();
                                }
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }

                            blockSkycount++;

                            try {
                                blockSkyTuple = block.get_next();
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }
                        } while (blockSkyTuple != null);

                        System.out.println("Read statistics "+PCounter.rcounter);
                        System.out.println("Write statistics "+PCounter.wcounter);

                        System.out.println("\n Number of Skyline candidates: " + blockSkycount);

                        try {
                            fscanBlock.close();
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        try {
                            block.close();
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }
                        break;

                    case 12:
                        FileScan fscan = null;

                        try {
                            fscan = new FileScan(heapFile, attrType, attrSize, (short) attrType.length, attrType.length, projlist, null);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        SystemDefs.JavabaseBM.flushPages();

                        PCounter.initialize();

                        SortFirstSky sort = null;
                        try {
                            sort = new SortFirstSky(attrType, attrType.length, attrSize, fscan, heapFile, pref_attr_lst, pref_attr_lst.length, _n_pages);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        int count = -1;
                        Tuple t = null;

                        System.out.println("\n -- Skyline candidates -- ");
                        do {
                            try {
                                if (t != null) {
                                    for (int i = 1; i <= t.noOfFlds(); i++) {
                                        System.out.print(t.getFloFld(i) + ", ");
                                    }
                                    System.out.println();
                                }
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }

                            count++;

                            try {
                                t = sort.get_next();
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }
                        } while (t != null);

                        System.out.println("Read statistics "+PCounter.rcounter);
                        System.out.println("Write statistics "+PCounter.wcounter);

                        System.out.println("\n Number of Skyline candidates: " + count);

                        try {
                            fscan.close();
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        try {
                            sort.close();
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        break;

                    case 13:
                        // call btree sky
                        break;

                    case 14:
                        // call btree sort sky
						break;

                    case 0:
                        break;
                }



            }
            catch(Exception e) {
                e.printStackTrace();
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("       !!         Something is wrong                    !!");
                System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            }
        }
        return true;
    }
}


/**
 * To get the integer off the command line
 */
class GetStuff {
    GetStuff() {}

    public static int getChoice () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;

        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            return -1;
        }
        catch (IOException e) {
            return -1;
        }

        return choice;
    }

    public static void getReturn () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));

        try {
            String ret = in.readLine();
        }
        catch (IOException e) {}
    }
}

public class ReadDriver implements  GlobalConst{

    public static void main(String [] argvs) {

        try{
            Driver driver = new Driver();
            driver.runTests();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }finally {

        }
    }

}
