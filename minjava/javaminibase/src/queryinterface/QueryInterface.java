package queryinterface;

import btree.ConstructPageException;
import btree.IntegerKey;
import btree.StringKey;
import btree.*;
import bufmgr.*;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import hash.*;
import heap.*;
import iterator.*;
import tests.TestDriver;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class QueryInterface extends TestDriver implements GlobalConst {

    protected String dbpath;
    protected String logpath;
    protected String dbName;

    private static RID rid;
    private static int _n_pages;
    private static Heapfile f = null;
    private static String METAFILE_POSTFIX = "-meta";
    private static BTreeFile bTreeUnclusteredFile = null;
    private static BTreeClusteredFile bTreeClusteredFile = null;
    private static ClusteredHashFile hashFile = null;
    private static UnclusteredHashFile unclusteredHashFile = null;
    private static Heapfile indexCatalogFile = null;
    private static boolean status = OK;
    private static int nColumns;
    private static AttrType[] attrType;
    private static short[] attrSizes;
    private static AttrType[] attrType2;
    private static short[] attrSizes2;
    private static int nColumns2;
    private static String[] attrNames;
    private static String[] attrNames2;
    private static AttrType[] indexAttrTypes;
    private static short[] indexAttrSizes;
    private static AttrType[] metaAttrTypes;
    private static short[] metaAttrSizes;
    private static Tuple indexTuple;
    private static Tuple metaTuple;
    private static short tSize = 34;
    private static short attrStringSize = 32;
    private static int[] pref_list;
    private static FldSpec[] projlist;
    private static FldSpec[] projlist2;
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private SystemDefs sysDef;
    private static boolean indexesCreated;
    private boolean dbClosed = true;
    public static final short CLUSTERED_HASH = 0;
    public static final short CLUSTERED_BTREE = 1;
    public static final short UNCLUSTERED_HASH = 2;
    public static final short UNCLUSTERED_BTREE = 3;
    public static final short NO_INDEX = 4;
    public static final String INDEX_FILE_NAME = "indexCatalog";
    public static String outputTableName = null;
    public static Boolean outputResultToTable = false;
    public static Heapfile outputTable = null;
    public static ArrayList<BTreeClusteredFile.RidChange> RidChanges = null;
    public static ArrayList<RidTuplePair> ridTuplePairs = null;

    private void menuInterface() {
        System.out.println("-------------------------- MENU --------------------------");
        System.out.println("[1]   Open database");
        System.out.println("[2]   Close current database");
        System.out.println("[3]   Create a new table");
        System.out.println("[4]   Create index");
        System.out.println("[5]   Insert data from file");
        System.out.println("[6]   Delete data from file");
        System.out.println("[7]   Output all tuples in a table");
        System.out.println("[8]   Output all keys stored in Index for a given attribute");
        System.out.println("[9]   Run SKYLINE operators");
        System.out.println("[10]  Run GROUPBY operators");
        System.out.println("[11]  Run JOIN operators");
        System.out.println("[12]  Run TOPKJOIN operators");
        System.out.println("[13]  Destroy Database");
        System.out.println("[14]  Set n_page = 5");
        System.out.println("[15]  Set n_page = 10");
        System.out.println("[16]  Set n_page = <your_wish>");
        System.out.println("\n[0]  Quit");
        System.out.print("Enter your choice :");
    }

    /**
     * QueryInterface Constructor
     */
    public QueryInterface() {
        super("main");
    }

    public boolean runTests() {
        initMetaAndIndexAttrs();
        boolean _pass = runAllTests();

        //Clean up again
        //cleanDB();
        if (!dbClosed) {
            close_database();
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completed successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    protected String testName() {
        return "Query Interface Driver";
    }

    private void initMetaAndIndexAttrs() {
        // For Index
        short size;
        indexTuple = new Tuple();
        indexAttrTypes = new AttrType[4];

        indexAttrTypes[0] = new AttrType(AttrType.attrString); // For relName
        indexAttrTypes[1] = new AttrType(AttrType.attrString); // For attrName
        indexAttrTypes[2] = new AttrType(AttrType.attrInteger);// For indexType
        indexAttrTypes[3] = new AttrType(AttrType.attrInteger);// For keyIndex

        indexAttrSizes = new short[2];
        indexAttrSizes[0] = (short) attrStringSize;
        indexAttrSizes[1] = (short) attrStringSize;

        try {
            indexTuple.setHdr((short) 4, indexAttrTypes, indexAttrSizes);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to set tuple header!");
        }

        size = indexTuple.size();

        indexTuple = new Tuple(size);
        try {
            indexTuple.setHdr((short) 4, indexAttrTypes, indexAttrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        //For Attrs
        metaTuple = new Tuple();
        metaAttrTypes = new AttrType[3];

        metaAttrTypes[0] = new AttrType(AttrType.attrString); // For attrName
        metaAttrTypes[1] = new AttrType(AttrType.attrInteger);// For indexType
        metaAttrTypes[2] = new AttrType(AttrType.attrInteger);// For attrLen

        metaAttrSizes = new short[1];
        metaAttrSizes[0] = (short) attrStringSize;

        try {
            metaTuple.setHdr((short) 3, metaAttrTypes, metaAttrSizes);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to set tuple header!");

        }

        size = metaTuple.size();

        metaTuple = new Tuple(size);
        try {
            metaTuple.setHdr((short) 3, metaAttrTypes, metaAttrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }
    }

    protected boolean runAllTests() {
        int choice = 1;
        String tname, fname, dbname;

        System.out.println();
        while (choice != 0) {
            menuInterface();
            status = OK;
            try {
                choice = GetStuff.getChoice();
                if (choice != 1 && dbClosed && choice != 0) {
                    System.out.println("Please open a database first!");

                } else {
                    switch (choice) {

                        case 1:
                            dbClosed = false;
                            System.out.print("Enter Database Name: ");
                            dbname = GetStuff.getStringChoice();
                            System.out.println();
                            open_database(dbname);
                            break;

                        case 2:
                            SystemDefs.JavabaseBM.flushPages();
                            close_database();
                            break;

                        case 3:
                            createTableMenu();
                            break;

                        case 4:
                            createIndexMenu();
                            break;

                        case 5:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Filename: ");
                            fname = GetStuff.getStringChoice();
                            System.out.print("Enter Tablename: ");
                            tname = GetStuff.getStringChoice();
                            System.out.println();
                            insert_data(tname, fname);
                            break;

                        case 6:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Filename: ");
                            fname = GetStuff.getStringChoice();
                            System.out.print("Enter Tablename: ");
                            tname = GetStuff.getStringChoice();
                            System.out.println();
                            delete_data(tname, fname);
                            break;

                        case 7:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Tablename: ");
                            tname = GetStuff.getStringChoice();
                            System.out.println();
                            printTable(tname);
                            break;

                        case 8:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Tablename: ");
                            tname = GetStuff.getStringChoice();
                            System.out.print("Enter Attribute Number: ");
                            int attrIndex = GetStuff.getChoice();
                            System.out.println();
                            printIndexKeys(tname, attrIndex);
                            break;

                        case 9:
                            SystemDefs.JavabaseBM.flushPages();
                            skylineMenu();
                            break;
                        case 10:
                            break;

                        case 11:
                            break;

                        case 12:
                            System.out.println("Enter your choice:\n[1] Hash-based Top-K Join\n[2] NRA-based Top-K Join");
                            int ch = GetStuff.getChoice();
                            if(ch == 1){

                            }else if(ch == 2){
                                System.out.println("Enter Query:");
                                String[] tokens = GetStuff.getStringChoice().split(" ");
                                int jAttr1 = Integer.valueOf(tokens[4]), jAttr2 = Integer.valueOf(tokens[7]), 
                                    mAttr1 = Integer.valueOf(tokens[5]), mAttr2 = Integer.valueOf(tokens[8]);
                                String fileName1 = tokens[3], fileName2 = tokens[6];
                                int k = Integer.valueOf(tokens[2]);
                                int n_pages = Integer.valueOf(tokens[9]);

                                createTable(fileName1, true, (short) 5, mAttr1);
                                createTable(fileName2, true, (short) 5, mAttr2);

                                getTableAttrsAndType(fileName1);
                                getSecondTableAttrsAndType(fileName2);
                                
                                String oTable = "null";
                                if(tokens.length > 10){
                                    oTable = tokens[11];
                                    createOutputTable(oTable, fileName1, fileName2, 1);
                                }

                                // printTable(fileName1);
                                // printTable(fileName2);

                                FldSpec[] joinList = new FldSpec[2];
                                FldSpec[] mergeList = new FldSpec[2];
                                
                                joinList[0] = new FldSpec(rel, jAttr1);
                                joinList[1] = new FldSpec(rel, jAttr2);
                                mergeList[0] = new FldSpec(rel, mAttr1);
                                mergeList[1] = new FldSpec(rel, mAttr2);

                                TopK_NRAJoin topK_NRAJoin = new TopK_NRAJoin(attrType, attrType.length, attrSizes, joinList[0], mergeList[0], 
                                attrType2, attrType2.length, attrSizes2, joinList[1], mergeList[1], fileName1, fileName2, k, n_pages, oTable);

                                PCounter.initialize();
                                try {
                                    SystemDefs.JavabaseBM.flushPages();
                                } catch (PageNotFoundException | BufMgrException | HashOperationException | PagePinnedException e) {
                                    e.printStackTrace();
                                }

                                topK_NRAJoin.computeTopK_NRA();

                                System.out.println("\nRead statistics "+PCounter.rcounter);
                                System.out.println("Write statistics "+PCounter.wcounter);

                                System.out.println("------------------- TEST 1 completed ---------------------\n");

                                System.out.println();
                                
                            }
                            break;

                        case 13:
                            System.out.print("Enter Database Name: ");
                            dbName = GetStuff.getStringChoice();
                            System.out.println();
                            dbpath = "/tmp/" + dbName + ".minibase-db";
                            logpath = "/tmp/" + dbName + ".minibase-log";
                            cleanDB();
                            break;

                        case 14:
                            _n_pages = 5;
                            break;

                        case 15:
                            _n_pages = 10;
                            break;

                        case 16:
                            System.out.println("Enter n_pages of your choice: ");
                            _n_pages = GetStuff.getChoice();
                            if (_n_pages <= 0)
                                break;
                            break;

                        case 0:
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("       !!               Something is wrong              !!");
                System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            }
        }
        return true;
    }

    private void skylineMenu() {
        try {
            System.out.print("Enter Tablename: ");
            String tname = GetStuff.getStringChoice();
            getTableAttrsAndType(tname);
            String tempRelName = createTempHeapFileForSkyline(tname);
            f = new Heapfile(tempRelName);
            System.out.println();
            prefMenu();
            indexesCreated = false;
            int choice = GetStuff.getChoice();
            switch (choice) {
                case 1:
                    pref_list = new int[]{1};
                    break;
                case 2:
                    pref_list = new int[]{1, 2};
                    break;
                case 3:
                    pref_list = new int[]{1, 3};
                    break;
                case 4:
                    pref_list = new int[]{1, 3, 5};
                    break;
                case 5:
                    pref_list = new int[]{1, 2, 3, 4, 5};
                    break;

                case 6:
                    System.out.println("Enter number of preferred attributes: ");
                    int prefLen = GetStuff.getChoice();
                    pref_list = new int[prefLen];
                    for (int i = 0; i < prefLen; i++) {
                        System.out.println("Enter preferred attribute index:");
                        pref_list[i] = GetStuff.getChoice();
                    }
                    System.out.println(Arrays.toString(pref_list));
                    break;
                case 0:
                    break;
            }

            if (choice == 0)
                return;

            pageMenu();

            choice = GetStuff.getChoice();
            switch (choice) {
                case 1:
                    _n_pages = 5;
                    break;

                case 2:
                    _n_pages = 10;
                    break;

                case 3:
                    System.out.println("Enter n_pages of your choice: ");
                    _n_pages = GetStuff.getChoice();
                    if (_n_pages <= 0)
                        break;
                    break;
                case 0:
                    break;
            }
            if (choice == 0)
                return;

            //choice = GetStuff.getChoice();
            while (choice != 0) {
                algoMenu();
                choice = GetStuff.getChoice();

                if(choice!=0){
                    outputTableMenu();
                }

                switch (choice) {
                    case 1:
                        // call nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runNestedLoopSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 2:
                        // call block nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBNLSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 3:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runSortFirstSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 4:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBtreeSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 5:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBTreeSortedSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 0:
                        f.deleteFile();
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println("       !!         Something is wrong                    !!");
            System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
            System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
    }

    private void outputTableMenu() {
        System.out.println("Do you want to store result in an output table");
        System.out.println("[1] YES");
        System.out.println("[2] NO");
        System.out.print("Enter your choice:");
        int yourChoice = GetStuff.getChoice();
        outputResultToTable = false;
        if (yourChoice == 1) {
            outputResultToTable = true;
            System.out.print("Enter Tablename: ");
            outputTableName = GetStuff.getStringChoice();
            try {
                outputTable = new Heapfile(outputTableName);
                outputTable.deleteFile();
                setTableMeta(outputTableName, attrType, attrSizes, attrNames);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            outputResultToTable = false;
            outputTableName = null;
        }
    }

    private void prefMenu() {
        System.out.println("[1]   Set pref = [1]");
        System.out.println("[2]   Set pref = [1,2]");
        System.out.println("[3]   Set pref = [1,3]");
        System.out.println("[4]   Set pref = [1,3,5]");
        System.out.println("[5]   Set pref = [1,2,3,4,5]");
        System.out.println("[6]   Set your own preference list of attributes");
        System.out.println("\n[0]  Quit!");
        System.out.print("Enter your choice :");
    }

    private void createTableMenu() throws IOException, InvalidTupleSizeException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        System.out.print("Enter Filename: ");
        String fname = GetStuff.getStringChoice();
        System.out.println();
        System.out.println();
        System.out.println("[1]   Create Clustered BT Index");
        System.out.println("[2]   Create Clustered Hash Index");
        System.out.println("[3]   Do not create index");
        System.out.print("Please enter your choice: ");
        int choice = GetStuff.getChoice();
        int attrInd = -1;
        if (choice == 1 || choice == 2) {
            System.out.print("Please enter attribute index :");
            attrInd = GetStuff.getChoice();
        }
        try {
            switch (choice) {
                case 1:
                    createTable(fname, true, CLUSTERED_BTREE, attrInd);
                    break;
                case 2:
                    createTable(fname, true, CLUSTERED_HASH, attrInd);
                    break;
                case 3:
                    createTable(fname, false, NO_INDEX, attrInd);
                    break;
                case 4:
                    createTable(fname, true, (short) 5, attrInd);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createIndexMenu() throws IOException, InvalidTupleSizeException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        System.out.print("Enter Table Name: ");
        String tname = GetStuff.getStringChoice();
        System.out.println();
        System.out.println();
        System.out.println("[1]   Create Unclustered BT Index");
        System.out.println("[2]   Create Unclustered Hash Index");
        System.out.print("Please enter your choice:");

        int choice = GetStuff.getChoice();
        int attrInd = -1;
        if (choice == 1 || choice == 2) {
            System.out.print("Please enter attribute index :");
            attrInd = GetStuff.getChoice();
        }

        try {
            switch (choice) {
                case 1:
                    createIndex(tname, UNCLUSTERED_BTREE, attrInd);
                    break;
                case 2:
                    createIndex(tname, UNCLUSTERED_HASH, attrInd);
                    break;
            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }
    }

    private void pageMenu() {
        System.out.println("[1]   Set n_page = 5");
        System.out.println("[2]   Set n_page = 10");
        System.out.println("[3]   Set n_page = <your_wish>");
        System.out.println("\n[0]  Quit!");
        System.out.print("Enter your choice :");
    }

    private void algoMenu() {
        System.out.println("[1]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[2]  Run Block Nested Loop on data with parameters ");
        System.out.println("[3]  Run Sort First Sky on data with parameters ");
        System.out.println("[4]  Run individual Btree Sky on data with parameters ");
        System.out.println("[5]  Run combined Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Enter your choice :");
    }

    public void cleanDB() {
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
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        System.out.println("Successfully deleted database " + dbName.toUpperCase());
    }

    private void close_database() {
        try {
            SystemDefs.JavabaseBM.flushPages();
            sysDef.JavabaseDB.closeDB();
            dbClosed = true;
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not close the database\n");
            e.printStackTrace();
        }

        System.out.println("Successfully closed database " + dbName.toUpperCase());
    }

    public void open_database(String nameRoot) {
        dbName = nameRoot;
//        dbpath = "/tmp/" + nameRoot + ".minibase-db";
//        logpath = "/tmp/" + nameRoot + ".minibase-log";
        dbpath = "..\\" + nameRoot + ".minibase-db";
        logpath = "..\\" + nameRoot + ".minibase-log";
        File f = new File(dbpath);

        if (f.exists() && !f.isDirectory()) {
            sysDef = new SystemDefs(dbpath, 0, 40000, "Clock");
            System.out.println("Successfully opened database " + dbName.toUpperCase());
        } else {
            sysDef = new SystemDefs(
                    dbpath, 50000, 40000, "Clock");
            System.out.println("Successfully created database " + dbName.toUpperCase());
        }

    }

    private void createOutputTable(String fileName, String fileName1, String fileName2, int attrIndex) throws IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException {

        //        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
        //                != SystemDefs.JavabaseBM.getNumBuffers()) {
        //            System.err.println("*** The heap file has left pages pinned\n");
        //            status = FAIL;
        //        }
        
            if (status == OK) {
    
                // Read data and construct tuples
                // getTableAttrsAndType(fileName1);

                // getSecondTableAttrsAndType(fileName2);
                
                AttrType[] oAttrTypes = new AttrType[attrType.length + attrType2.length];
                System.arraycopy(attrType, 0, oAttrTypes, 0, attrType.length);
                System.arraycopy(attrType, 0, oAttrTypes, attrType.length, attrType2.length);

                short[] oAttrSize = new short[attrType.length + attrType2.length];
                System.arraycopy(attrSizes, 0, oAttrSize, 0, attrSizes.length);
                System.arraycopy(attrSizes2, 0, oAttrSize, attrSizes.length, attrSizes2.length);

                String[] oAttrName = new String[attrNames.length + attrNames2.length];
                System.arraycopy(attrNames, 0, oAttrName, 0, attrNames.length);
                System.arraycopy(attrNames2, 0, oAttrName, attrNames.length, attrNames2.length);

                // int nColumns = attrType.length + attrType2.length;

                try {
                    f = new Heapfile(fileName);
                    f.deleteFile();
                    f = new Heapfile(fileName);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                setTableMeta(fileName, oAttrTypes, oAttrSize, oAttrName);
            }
        }

    private void createTable(String fileName, Boolean createIndex, short clusteredIndexType, int attrIndex) throws IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException {

//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;fileName
//        }

        if (status == OK) {

            // Read data and construct tuples
            setAttrDesc(fileName);
            File file = new File("../../data/" + fileName + ".csv");
            // File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + fileName + ".csv");

            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < nColumns; i++) {
                sc.nextLine();
            }

            try {
                if (createIndex) {
                    short keySize = 4;
                    if (attrType[attrIndex - 1].attrType == AttrType.attrString) {
                        keySize = attrStringSize;
                    }
                    if (clusteredIndexType == CLUSTERED_BTREE || clusteredIndexType == 5) {
                        bTreeClusteredFile = new BTreeClusteredFile(fileName, attrType[attrIndex - 1].toInt(), keySize, attrIndex, 0, (short) nColumns, attrType, attrSizes);
                    } else {
                        hashFile = new ClusteredHashFile(fileName, 75, attrType[attrIndex - 1].toInt(), keySize, (short) nColumns, attrType, attrSizes);
                    }
                } else {
                    f = new Heapfile(fileName);
                }
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not create file\n");
                e.printStackTrace();
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            int value;
            int count = 0;
            ClusteredHashRecord rec;
            PCounter.initialize();
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");

                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            if(clusteredIndexType == 5 && i == attrIndex - 1)
                                value = -Integer.parseInt(row[i]);
                            else
                                value = Integer.parseInt(row[i]);
                            tuple1.setIntFld(i + 1, value);
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    if (createIndex) {
                        if (clusteredIndexType == CLUSTERED_BTREE || clusteredIndexType == 5) {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
                                bTreeClusteredFile.insert(key, tuple1);
                            } else {
                                StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
                                bTreeClusteredFile.insert(key, tuple1);
                            }

                        } else {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                                rec = new ClusteredHashRecord(key, tuple1);
                                hashFile.insertRecord(key, rec);
                            } else {
                                hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                                rec = new ClusteredHashRecord(key, tuple1);
                                hashFile.insertRecord(key, rec);
                            }

                        }

                    } else {
                        rid = f.insertRecord(tuple1.returnTupleByteArray());
                    }
                    count++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                if (createIndex) {
                    addIndexToCatalog(fileName, attrNames[attrIndex - 1], clusteredIndexType, attrIndex);
                }
                PCounter.printStats();
                System.out.println("New table created " + fileName.toUpperCase());
                System.out.println("Record count: " + count);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void createIndex(String tableName, short unclusteredIndexType, int attrIndex) throws IOException, FieldNumberOutOfBoundException {

        Tuple t;
        int count;
        //Check if clustered index exists
        int indexTypeIfExists = findIfIndexExists(tableName, -1);
        getTableAttrsAndType(tableName);
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned ***\n");
//            status = FAIL;
//        }

        int indexOnAttrExists = findIfIndexExists(tableName, attrIndex);

        if (status != OK) {
            return;
        }

        if (indexOnAttrExists != NO_INDEX) {
            String indexType = "";
            switch (indexOnAttrExists) {
                case 0:
                    indexType = "CLUSTERED_HASH";
                    break;
                case 1:
                    indexType = "CLUSTERED_BTREE";
                    break;
                case 2:
                    indexType = "UNCLUSTERED_HASH";
                    break;
                case 3:
                    indexType = "UNCLUSTERED_BTREE";
                    break;
            }
            System.out.println("Index already exists on this attribute of type: " + indexType);
            return;
        }

        String indexFileName = tableName + '-' + unclusteredIndexType + '-' + attrIndex;

        try {
            short keySize = 4;
            if (attrType[attrIndex - 1].attrType == AttrType.attrString) {
                keySize = attrStringSize;
            }
            if (unclusteredIndexType == UNCLUSTERED_HASH) {
                unclusteredHashFile = new UnclusteredHashFile(indexFileName, attrType[attrIndex - 1].toInt(), keySize, 75);
            } else {
                bTreeUnclusteredFile = new BTreeFile(indexFileName, attrType[attrIndex - 1].toInt(), keySize, 0);
            }
        } catch (Exception e) {
            System.out.println("Failed to initialize unclustered index file!");
            e.printStackTrace();
        }
        PCounter.initialize();
        try {
            if (indexTypeIfExists == NO_INDEX) {
                try {
                    f = new Heapfile(tableName);
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Could not create heap file ***\n");
                    e.printStackTrace();
                }

                Scan scan = null;
                RID rid = new RID();

                try {
                    scan = f.openScan();
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Error opening scan\n");
                    e.printStackTrace();
                }

                t = new Tuple();

                try {
                    t = scan.getNext(rid);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                while (t != null) {
                    try {
                        t.setHdr((short) nColumns, attrType, attrSizes);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }


                    try {
                        if (unclusteredIndexType == UNCLUSTERED_HASH) {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                                unclusteredHashFile.insertRecord(key, rid);
                            } else {
                                hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                                unclusteredHashFile.insertRecord(key, rid);
                            }
                        } else {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                                bTreeUnclusteredFile.insert(key, rid);

                            } else {
                                StringKey key = new StringKey(t.getStrFld(attrIndex));
                                bTreeUnclusteredFile.insert(key, rid);
                            }
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        return;
                    }

                    try {
                        t = scan.getNext(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                // clean up
                try {
                    scan.closescan();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            } else if (indexTypeIfExists == CLUSTERED_BTREE) {
                bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                BTClusteredFileScan scan = null;
                try {
                    scan = bTreeClusteredFile.new_scan(null, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeyNotMatchException e) {
                    e.printStackTrace();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                }

                KeyDataEntry data = null;
                rid = new RID();
                data = scan.get_next(rid);
                t = null;

                while (data != null) {
                    try {
                        t = ((Tuple) ((ClusteredLeafData) data.data).getData());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (unclusteredIndexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        }
                    } else {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);

                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        }
                    }

                    try {
                        data = scan.get_next(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeClusteredFile.close();

            } else if (indexTypeIfExists == CLUSTERED_HASH) {
                RID rid = new RID();
                hashFile = new ClusteredHashFile(tableName, (short) attrType.length, attrType, attrSizes);
                ClusteredHashFileScan fscan = null;
                fscan = hashFile.newScan(null, null);
                t = null;
                t = fscan.getNextTuple(rid);

                while (t != null) {
//                    try {
//                        t.print(attrType);
//                    } catch (Exception e) {
//                        status = FAIL;
//                        e.printStackTrace();
//                    }

                    if (unclusteredIndexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        }
                    } else {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);

                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        }
                    }

                    try {
                        t = fscan.getNextTuple(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                hashFile.close();
            }
            System.out.println("Successfully created unclustered index!");
            PCounter.printStats();
            addIndexToCatalog(tableName, attrNames[attrIndex - 1], unclusteredIndexType, attrIndex);
            if (unclusteredIndexType == UNCLUSTERED_HASH) {
                unclusteredHashFile.close();
            } else {
                bTreeUnclusteredFile.close();
            }
        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to print results");
            e.printStackTrace();
        }
    }

    private void printIndexKeys(String tableName, int attrIndex) throws IOException, FieldNumberOutOfBoundException {

        Tuple t;
        int count = 0;
        //Check if clustered index exists
        getTableAttrsAndType(tableName);
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned ***\n");
//            status = FAIL;
//        }

        int indexOnAttrExists = findIfIndexExists(tableName, attrIndex);

        if (status != OK) {
            return;
        }

        if (indexOnAttrExists == NO_INDEX) {
            System.out.println("No Index exists on this attribute: ");
            return;
        }

        String unclusteredIndexFileName = tableName + '-' + indexOnAttrExists + '-' + attrIndex;

        short keySize = 4;
        if (attrType[attrIndex - 1].attrType == AttrType.attrString) {
            keySize = attrStringSize;
        }

        System.out.println("***KEYS***");
        System.out.println("----------");
        rid = new RID();
        PCounter.initialize();
        try {
            if (indexOnAttrExists == CLUSTERED_HASH) {
                hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                ClusteredHashFileScan fscan = null;
                fscan = hashFile.newScan(null, null);
                t = null;
                t = fscan.getNextTuple(rid);


                while (t != null) {
                    try {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            System.out.println(t.getIntFld(attrIndex));
                        } else {
                            System.out.println(t.getStrFld(attrIndex));
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    count++;

                    try {
                        t = fscan.getNextTuple(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                hashFile.close();
            } else if (indexOnAttrExists == CLUSTERED_BTREE) {
                bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                BTClusteredFileScan scan = null;
                try {
                    scan = bTreeClusteredFile.new_scan(null, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeyNotMatchException e) {
                    e.printStackTrace();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                }

                KeyDataEntry data = null;
                data = scan.get_next(rid);
//                t = null;


                while (data != null) {
                    try {
//                        t = ((Tuple) ((ClusteredLeafData) data.data).getData());
                        System.out.println(data.key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    count++;

//                    if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
//                        System.out.println(t.getIntFld(attrIndex));
//                    } else {
//                        System.out.println(t.getStrFld(attrIndex));
//                    }

                    try {
                        data = scan.get_next(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeClusteredFile.close();

            } else if (indexOnAttrExists == UNCLUSTERED_HASH) {
                unclusteredHashFile = new UnclusteredHashFile(unclusteredIndexFileName);
                UnclusteredHashRecord record = null;
                UnclusteredHashFileScan scan = unclusteredHashFile.newScan(null, null);
                try {
                    record = scan.getNextRecord();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (PageUnpinnedException e) {
                    e.printStackTrace();
                } catch (InvalidFrameNumberException e) {
                    e.printStackTrace();
                } catch (HashEntryNotFoundException e) {
                    e.printStackTrace();
                } catch (ReplacerException e) {
                    e.printStackTrace();
                }

                while (record != null) {
                    try {
                        System.out.println(record.toString());
                        count++;
                        record = scan.getNextRecord();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (PageUnpinnedException e) {
                        e.printStackTrace();
                    } catch (InvalidFrameNumberException e) {
                        e.printStackTrace();
                    } catch (HashEntryNotFoundException e) {
                        e.printStackTrace();
                    } catch (ReplacerException e) {
                        e.printStackTrace();
                    }
                }
                unclusteredHashFile.close();
            } else if (indexOnAttrExists == UNCLUSTERED_BTREE) {
                bTreeUnclusteredFile = new BTreeFile(unclusteredIndexFileName);
                BTFileScan scan = bTreeUnclusteredFile.new_scan(null, null);

                KeyDataEntry data = null;

                data = scan.get_next();

                while (data != null) {
                    try {
                        System.out.println(data.key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    count++;

                    try {
                        data = scan.get_next();
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeUnclusteredFile.close();
            }
            PCounter.printStats();
            System.out.println("AT THE END OF SCAN!");
            System.out.println("*** TOTAL KEYS " + count + " ***");

        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to print index keys");
            e.printStackTrace();
        }
    }

    private void addIndexToCatalog(String relName, String attrName, short indexType, int attrIndex) throws IOException, FieldNumberOutOfBoundException, HFDiskMgrException, InvalidTupleSizeException, HFException, InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {
        try {
            indexCatalogFile = new Heapfile(INDEX_FILE_NAME);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        indexTuple.setStrFld(1, relName);
        indexTuple.setStrFld(2, attrName);
        indexTuple.setIntFld(3, (int) indexType);
        indexTuple.setIntFld(4, attrIndex);

        indexCatalogFile.insertRecord(indexTuple.returnTupleByteArray());
    }

    // Return index type
    private int findIfIndexExists(String relName, int attrIndex) {
        // Read data and construct tuples
        FileScan fscan = null;
        int indexType = NO_INDEX;

        FldSpec[] projections = new FldSpec[indexAttrTypes.length];

        for (int i = 0; i < indexAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(INDEX_FILE_NAME, indexAttrTypes, indexAttrSizes, (short) indexAttrTypes.length, indexAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        while (t != null) {
            try {
                if (attrIndex == -1 && t.getStrFld(1).equals(relName) && (t.getIntFld(3) == (short) 5 || t.getIntFld(3) == CLUSTERED_BTREE || t.getIntFld(3) == CLUSTERED_HASH)) {
                    indexType = t.getIntFld(3);
                    break;
                }
                if (attrIndex != -1 && t.getStrFld(1).equals(relName) && t.getIntFld(4) == attrIndex) {
                    indexType = t.getIntFld(3);
                    break;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        return indexType;
    }

    private List<IndexDesc> getAllIndexesForRel(String relName) {
        // Read data and construct tuples
        FileScan fscan = null;

        FldSpec[] projections = new FldSpec[indexAttrTypes.length];

        for (int i = 0; i < indexAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(INDEX_FILE_NAME, indexAttrTypes, indexAttrSizes, (short) indexAttrTypes.length, indexAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        List<IndexDesc> allIndexes = new ArrayList<IndexDesc>();
        while (t != null) {
            try {
                if (t.getStrFld(1).equals(relName)) {
                    IndexDesc id = new IndexDesc(t.getIntFld(3), t.getIntFld(4));
                    allIndexes.add(id);
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

//            nColumns++;

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        return allIndexes;
    }

    private void printTable(String tableName) throws IOException {
        Tuple t;
        int count;
        int indexTypeIfExists = findIfIndexExists(tableName, -1);
        getTableAttrsAndType(tableName);
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;
//        }

        if (status != OK) {
            return;
        }
        rid = new RID();
        PCounter.initialize();
        try {
            if (indexTypeIfExists == NO_INDEX) {
                try {
                    f = new Heapfile(tableName);
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Could not create heap file\n");
                    e.printStackTrace();
                }

                FileScan fscan = null;

                try {
                    fscan = new FileScan(tableName, attrType, attrSizes, (short) nColumns, nColumns, projlist, null);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                count = 0;
                t = null;
                try {
                    t = fscan.get_next();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
                while (t != null) {
                    try {
                        t.print(attrType);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    count++;

                    try {
                        t = fscan.get_next();
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }


                System.out.println("Record count: " + count);

                // clean up
                try {
                    fscan.close();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            } else if (indexTypeIfExists == CLUSTERED_BTREE || indexTypeIfExists == (short) 5) {
                bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                BTClusteredFileScan scan = null;
                try {
                    scan = bTreeClusteredFile.new_scan(null, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeyNotMatchException e) {
                    e.printStackTrace();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                }

                KeyDataEntry data = null;
                data = scan.get_next(rid);

                count = 0;
                while (data != null) {
                    if (data != null) {
                        try {
                            ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    count++;

                    try {
                        data = scan.get_next(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeClusteredFile.close();
                System.out.println("Record count: " + count);

            } else if (indexTypeIfExists == CLUSTERED_HASH) {
                hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                ClusteredHashFileScan fscan = null;
                fscan = hashFile.newScan(null, null);
                t = null;
                t = fscan.getNextTuple(rid);

                count = 0;
                while (t != null) {
                    try {
                        t.print(attrType);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    count++;

                    try {
                        t = fscan.getNextTuple(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                hashFile.close();
                PCounter.printStats();
                System.out.println("Record count: " + count);
            }
        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to print results");
            e.printStackTrace();
        }

    }

    private void insert_data(String tableName, String filename) throws IOException, FieldNumberOutOfBoundException {
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;
//        }


        if (status == OK) {
            getTableAttrsAndType(tableName);
//          File file = new File("../../data/" + filename + ".csv");
            File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + filename + ".csv");
            Scanner sc = new Scanner(file);
            int attrIndex = -1;
            int indexTypeIfExists = findIfIndexExists(tableName, -1);
            List<IndexDesc> allIndexes = getAllIndexesForRel(tableName);

            if (indexTypeIfExists != NO_INDEX) {
                for (int i = 0; i < allIndexes.size(); i++) {
                    if (allIndexes.get(i).indexType == indexTypeIfExists) {
                        attrIndex = allIndexes.get(i).attrIndex;
                        break;
                    }
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                    RidChanges = new ArrayList<BTreeClusteredFile.RidChange>();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                } else {
                    f = new Heapfile(tableName);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                }
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not open file ***\n");
                e.printStackTrace();
            }

            String columnMetaData;
            int attributeType;

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim().split(",")[1];
                if (columnMetaData.equals("INT")) {
                    attributeType = AttrType.attrInteger;
                } else {
                    attributeType = AttrType.attrString;
                }
                if (attributeType != attrType[i].attrType) {
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            int count = 0;
            int value;
            RidTuplePair ridtuple;
            PCounter.initialize();
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");
                tuple1 = new Tuple(size);
                try {
                    tuple1.setHdr((short) nColumns, attrType, attrSizes);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    status = FAIL;
                    e.printStackTrace();
                }
                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            value = Integer.parseInt(row[i]);
                            tuple1.setIntFld(i + 1, value);
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    if (indexTypeIfExists == CLUSTERED_BTREE) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
                            RidChanges = bTreeClusteredFile.insert(key, tuple1);
                        } else {
                            StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
                            RidChanges = bTreeClusteredFile.insert(key, tuple1);
                        }
                        IndexDesc index;
                        for (int i = 0; i < allIndexes.size(); i++) {
                            index = allIndexes.get(i);
                            if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                                bulkRestructureUnclustered(RidChanges, tableName, index.indexType, index.attrIndex,tuple1);
                            }
                        }
                    } else if (indexTypeIfExists == CLUSTERED_HASH) {
                        ridtuple = new RidTuplePair();
                        ridtuple.tuple = tuple1;
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                            ridtuple.rid = hashFile.insertRecord(key, tuple1);
                        } else {
                            hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                            ridtuple.rid = hashFile.insertRecord(key, tuple1);
                        }
                        ridTuplePairs.add(ridtuple);
                    } else {
                        ridtuple = new RidTuplePair();
                        ridtuple.rid = f.insertRecord(tuple1.returnTupleByteArray());
                        ridtuple.tuple = tuple1;
                        ridTuplePairs.add(ridtuple);
                    }
                    count++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile.close();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile.close();
                }
                System.out.println("New records inserted: " + count);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (indexTypeIfExists != CLUSTERED_BTREE) {
                IndexDesc index;
                for (int i = 0; i < allIndexes.size(); i++) {
                    index = allIndexes.get(i);
                    if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                        bulkUpdateUnclustered(ridTuplePairs, tableName, index.indexType, index.attrIndex, true);
                    }
                }
            }
            PCounter.printStats();

        }
    }

    private Boolean bulkRestructureUnclustered(ArrayList<BTreeClusteredFile.RidChange> ridChanges, String tableName, int indexType, int attrIndex, Tuple tuplerecord) throws FileNotFoundException {
        String indexFileName = tableName + '-' + indexType + '-' + attrIndex;

        try {
            if (indexType == UNCLUSTERED_HASH) {
                unclusteredHashFile = new UnclusteredHashFile(indexFileName);
            } else {
                bTreeUnclusteredFile = new BTreeFile(indexFileName);
                bTreeUnclusteredFile = new BTreeFile(indexFileName);
            }
        } catch (Exception e) {
            System.out.println("Failed to initialize unclustered index file!");
            e.printStackTrace();
        }
        Boolean isNewUpdate = false;
        BTreeClusteredFile.RidChange ridChange = null;
        Tuple t;
        for (int i = 0; i < ridChanges.size(); i++) {
            ridChange = ridChanges.get(i);
            try {
                if(ridChange.keyData!=null){
                    t = ((Tuple) ((ClusteredLeafData) ridChange.keyData.data).getData());
                }else{
                    t = tuplerecord;
                }
                if (indexType == UNCLUSTERED_HASH) {
                    if (ridChange.newRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid != null && ridChange.newRid != null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        }
                    }
                } else {
                    if (ridChange.newRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);
                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid != null && ridChange.newRid != null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);

                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);
                        }
                    }
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        try {
            if (indexType == UNCLUSTERED_HASH) {
                unclusteredHashFile.close();
            } else {
                bTreeUnclusteredFile.close();
            }
            System.out.println("Unclustered index updated on attr " + attrIndex);
        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to close files");
            e.printStackTrace();
        }
        return isNewUpdate;
    }

    private void bulkUpdateUnclustered(ArrayList<RidTuplePair> rtPair, String tableName, int indexType, int attrIndex, Boolean isInsert) throws FileNotFoundException {
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;
//        }


        if (status == OK) {
            String indexFileName = tableName + '-' + indexType + '-' + attrIndex;

            try {
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile = new UnclusteredHashFile(indexFileName);
                } else {
                    bTreeUnclusteredFile = new BTreeFile(indexFileName);
                }
            } catch (Exception e) {
                System.out.println("Failed to initialize unclustered index file!");
                e.printStackTrace();
            }

            RidTuplePair ridtuple = null;
            for (int i = 0; i < rtPair.size(); i++) {
                ridtuple = rtPair.get(i);
                try {
                    if (indexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(ridtuple.tuple.getIntFld(attrIndex));
                            if (isInsert) {
                                unclusteredHashFile.insertRecord(key, ridtuple.rid);
                            } else {
                                unclusteredHashFile.deleteRecord(key, ridtuple.rid);
                            }
                        } else {
                            hash.StringKey key = new hash.StringKey(ridtuple.tuple.getStrFld(attrIndex));
                            if (isInsert) {
                                unclusteredHashFile.insertRecord(key, ridtuple.rid);
                            } else {
                                unclusteredHashFile.deleteRecord(key, ridtuple.rid);
                            }
                        }
                    } else {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(ridtuple.tuple.getIntFld(attrIndex));
                            if (isInsert) {
                                bTreeUnclusteredFile.insert(key, ridtuple.rid);
                            } else {
                                bTreeUnclusteredFile.Delete(key, ridtuple.rid);
                            }
                        } else {
                            StringKey key = new StringKey(ridtuple.tuple.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, ridtuple.rid);
                            if (isInsert) {
                                bTreeUnclusteredFile.insert(key, ridtuple.rid);
                            } else {
                                bTreeUnclusteredFile.Delete(key, ridtuple.rid);
                            }
                        }
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile.close();
                } else {
                    bTreeUnclusteredFile.close();
                }
            } catch (Exception e) {
                status = FAIL;
                System.out.println("Failed to close files");
                e.printStackTrace();
            }

            try {
                System.out.println("Unclustered index updated on attr " + attrIndex);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    private void delete_data(String tableName, String filename) throws IOException, FieldNumberOutOfBoundException, TupleUtilsException, InvalidTupleSizeException, hash.ConstructPageException, InvalidSlotNumberException, InvalidTypeException, UnknowAttrType, UnpinPageException, DeleteRecException, IndexSearchException, RedistributeException, PinPageException, FreePageException, DeleteFashionException, LeafRedistributeException, IndexInsertRecException, IndexFullDeleteException, InsertRecException, KeyNotMatchException, LeafDeleteException, RecordNotFoundException, ConstructPageException, IteratorException {

//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;
//        }

        if (status == OK) {

            getTableAttrsAndType(tableName);
//          File file = new File("../../data/" + filename + ".csv");
            File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + filename + ".csv");
            Scanner sc = new Scanner(file);
            int attrIndex = 1;
            int indexTypeIfExists = findIfIndexExists(tableName, -1);
            List<IndexDesc> allIndexes = getAllIndexesForRel(tableName);

            if (indexTypeIfExists != NO_INDEX) {
                for (int i = 0; i < allIndexes.size(); i++) {
                    if (allIndexes.get(i).indexType == indexTypeIfExists) {
                        attrIndex = allIndexes.get(i).attrIndex;
                        break;
                    }
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                    RidChanges = new ArrayList<BTreeClusteredFile.RidChange>();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                } else {
                    f = new Heapfile(tableName);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                }
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not open file ***\n");
                e.printStackTrace();
            }

            String columnMetaData;
            int attributeType;

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim().split(",")[1];
                if (columnMetaData.equals("INT")) {
                    attributeType = AttrType.attrInteger;
                } else {
                    attributeType = AttrType.attrString;
                }
                if (attributeType != attrType[i].attrType) {
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            Integer count = 0;
            Boolean match, removed = false, result;
            Scan scan;
            RID rid;
            RidTuplePair ridtuple;
            PCounter.initialize();
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");
                tuple1 = new Tuple(size);
                try {
                    tuple1.setHdr((short) nColumns, attrType, attrSizes);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    status = FAIL;
                    e.printStackTrace();
                }
                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            tuple1.setIntFld(i + 1, Integer.parseInt(row[i]));
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                if (indexTypeIfExists == NO_INDEX) {
                    scan = null;
                    rid = new RID();

                    try {
                        scan = f.openScan();
                    } catch (Exception e) {
                        status = FAIL;
                        System.err.println("*** Error opening scan\n");
                        e.printStackTrace();
                    }

                    Tuple t = new Tuple();

                    try {
                        t = scan.getNext(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    while (t != null) {
                        try {
                            t.setHdr((short) nColumns, attrType, attrSizes);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }
                        match = true;
                        try {
                            result = TupleUtils.Equal(tuple1, t, attrType, row.length);
                            if (!result) {
                                match = false;
                            }
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                            return;
                        }


                        if (match) {
                            try {
                                ridtuple = new RidTuplePair();
                                removed = f.deleteRecord(rid);

                                if (removed) {
                                    count++;
                                    ridtuple.rid = rid;
                                    ridtuple.tuple = tuple1;
                                    ridTuplePairs.add(ridtuple);
                                    System.out.print("Successfully deleted: ");
                                } else {
                                    System.out.print("Failed to remove: ");
                                }
                                t.print(attrType);
                                // break;
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }
                        }

                        try {
                            t = scan.getNext(rid);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }
                    }
                    // clean up
                    try {
                        scan.closescan();
                    } catch (Exception e) {
                        status = FAIL;
//                        e.printStackTrace();
                    }
                } else if (indexTypeIfExists == CLUSTERED_BTREE) {
                    try {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
                            RidChanges = bTreeClusteredFile.Delete(key, tuple1);
                        } else {
                            StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
                            RidChanges = bTreeClusteredFile.Delete(key, tuple1);
                        }
                        IndexDesc index;
                        for (int i = 0; i < allIndexes.size(); i++) {
                            index = allIndexes.get(i);
                            if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                                removed = bulkRestructureUnclustered(RidChanges, tableName, index.indexType, index.attrIndex,tuple1);
                            }
                        }
                        for (int i=0; i <RidChanges.size();i++){
                            if(RidChanges.get(i).newRid == null){
                                count ++;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    ridtuple = new RidTuplePair();
                    ridtuple.tuple = tuple1;
                    try {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                            ridtuple.rid = hashFile.deleteRecord(key, tuple1);
                        } else {
                            hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                            ridtuple.rid = hashFile.deleteRecord(key, tuple1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (ridtuple.rid != null) {
                        count++;
                        ridTuplePairs.add(ridtuple);
                    }
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile.close();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile.close();
                }
                System.out.println("Records removed: " + count);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (indexTypeIfExists != CLUSTERED_BTREE) {
                IndexDesc index;
                for (int i = 0; i < allIndexes.size(); i++) {
                    index = allIndexes.get(i);
                    if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                        bulkUpdateUnclustered(ridTuplePairs, tableName, index.indexType, index.attrIndex, false);
                    }
                }
            }
            PCounter.printStats();
        }
    }

    private void bulkRemoveUnclustered(String filename, String tableName, int indexType, int attrIndex) throws FileNotFoundException {
        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }


        if (status == OK) {
//          File file = new File("../../data/" + filename + ".csv");
            File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + filename + ".csv");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < nColumns; i++) {
                sc.nextLine();
            }

            String indexFileName = tableName + '-' + indexType + '-' + attrIndex;

            try {
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile = new UnclusteredHashFile(indexFileName);
                } else {
                    bTreeUnclusteredFile = new BTreeFile(indexFileName);
                }
            } catch (Exception e) {
                System.out.println("Failed to initialize unclustered index file!");
                e.printStackTrace();
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            int value;
            UnclusteredHashFileScan hfScan;
            BTFileScan btScan;
            UnclusteredHashRecord record = null;
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");


                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            value = Integer.parseInt(row[i]);
                            tuple1.setIntFld(i + 1, value);
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }


                try {
                    if (indexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                            hfScan = unclusteredHashFile.newScan(key, key);
                            record = hfScan.getNextRecord();
                            rid = record.getRid();
                            unclusteredHashFile.deleteRecord(key, rid);
                        } else {
                            hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                            hfScan = unclusteredHashFile.newScan(key, key);
                            record = hfScan.getNextRecord();
                            rid = record.getRid();
                            unclusteredHashFile.deleteRecord(key, rid);
                        }
                    } else {
//                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
//                            IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
//                            btScan = bTreeUnclusteredFile.new_scan(key,key);
//                            KeyDataEntry kd = btScan.get_next();
//                            bTreeUnclusteredFile.Delete();
//                        } else {
//                            StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
//                            bTreeUnclusteredFile.insert(key, rid);
//                        }
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                sc.close();
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile.close();
                } else {
                    bTreeUnclusteredFile.close();
                }
            } catch (Exception e) {
                status = FAIL;
                System.out.println("Failed to close scan");
                e.printStackTrace();
            }

            try {
                System.out.println("Unclustered index updated on attr " + attrIndex);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    public static void runNestedLoopSky(String hf, Boolean outputResultToTable, String outputTableName) throws
            PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanNested = initialiseFileScan(hf);
        NestedLoopsSky nested = null;
        try {
            nested = new NestedLoopsSky(attrType, attrType.length, attrSizes, fscanNested, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(nested, outputResultToTable, outputTableName);

        try {
            nested.close();
            fscanNested.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runBNLSky(String hf, Boolean outputResultToTable, String outputTableName) throws
            PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanBlock = initialiseFileScan(hf);
        Iterator block = null;
        try {
            block = new BlockNestedLoopsSky(attrType, attrType.length, attrSizes, fscanBlock, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(block, outputResultToTable, outputTableName);

        try {
            fscanBlock.close();
            block.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runSortFirstSky(String hf, Boolean outputResultToTable, String outputTableName) throws
            PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscan = initialiseFileScan(hf);
        Iterator sort = null;
        try {
            sort = new SortFirstSky(attrType, attrType.length, attrSizes, fscan, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(sort, outputResultToTable, outputTableName);

        try {
            sort.close();
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
        }
    }

    private void runBtreeSky(String hf, Boolean outputResultToTable, String outputTableName) throws Exception {
        System.out.println("Running BTreeSky");
        System.out.println("DataFile: " + hf);
        System.out.println("Preference list: " + Arrays.toString(pref_list));
        System.out.println("Number of pages: " + _n_pages);
        System.out.println("Pref list length: " + pref_list.length);
        if (!indexesCreated) {
            BTreeUtil.createBtreesForPrefList(hf, f, attrType, attrSizes, pref_list);
            indexesCreated = true;
        }

        // autobox to IndexFile type
        IndexFile[] index_file_list = BTreeUtil.getBTrees(pref_list);
        SystemDefs.JavabaseBM.flushPages();

        BTreeSky btreesky = new BTreeSky(attrType, nColumns, attrSizes, null, hf, pref_list,
                pref_list.length, index_file_list, _n_pages);

        PCounter.initialize();
        String tempHeapFile = btreesky.findBTreeSky();

        if (tempHeapFile != null) {
            runSortFirstSky(tempHeapFile, outputResultToTable, outputTableName);
            Heapfile tempHF = new Heapfile(tempHeapFile);
            tempHF.deleteFile();
        }

        System.out.println("BTreeSky Complete\n");
    }

    public void runBTreeSortedSky(String hf, Boolean outputResultToTable, String outputTableName) {
        try {
            BTreeCombinedIndex obj = new BTreeCombinedIndex();
            IndexFile indexFile = obj.combinedIndex(hf, attrType, attrSizes, pref_list, pref_list.length);
            System.out.println("Index created!");
            SystemDefs.JavabaseBM.flushPages();

            System.out.println("CombinedBTreeIndex scanning");
            String fileName = BTreeCombinedIndex.random_string1;

            BTreeSortedSky btree = new BTreeSortedSky(attrType, attrType.length, attrSizes, null, fileName, pref_list, pref_list.length, indexFile, _n_pages);
            PCounter.initialize();
            String tempHeapFile = btree.computeSkyline();

            if (tempHeapFile != null) {
                runSortFirstSky(tempHeapFile, outputResultToTable, outputTableName);
                Heapfile tempHF = new Heapfile(tempHeapFile);
                tempHF.deleteFile();
            }

            System.out.println("BTreeSortSky Complete");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getNextAndPrintAllSkyLine(Iterator iter, Boolean outputResultToTable, String outputTableName) {
// this needs to be before the fn call since call to any algo 1,2,3 from 4,5 reinitializes counter
//        PCounter.initialize();
        outputTable = null;
        if (outputResultToTable && outputTableName != null) {
            try {
                outputTable = new Heapfile(outputTableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        int count = -1;
        Tuple tuple1 = null;
        System.out.println("\n -- Skyline Objects -- ");
        do {
            try {
                if (tuple1 != null) {
                    tuple1.print(attrType);
                    if (outputTable != null && outputResultToTable && outputTableName != null) {
                        outputTable.insertRecord(tuple1.returnTupleByteArray());
                    }
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count++;

            try {
                tuple1 = iter.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        } while (tuple1 != null);

        System.out.println("\nRead statistics " + PCounter.rcounter);
        System.out.println("Write statistics " + PCounter.wcounter);

        System.out.println("\nNumber of Skyline objects: " + count + "\n");
    }

    public static FileScan initialiseFileScan(String hf) {
        FileScan fscan = null;

        try {
            fscan = new FileScan(hf, attrType, attrSizes, (short) attrType.length, attrType.length, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return fscan;
    }

    private void setAttrDesc(String tableName) throws IOException, FieldNumberOutOfBoundException {
        try {
            f = new Heapfile(tableName + METAFILE_POSTFIX);
            f.deleteFile();
            f = new Heapfile(tableName + METAFILE_POSTFIX);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        File file = new File("../../data/" + tableName + ".csv");
        // File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + tableName + ".csv");
        Scanner sc = new Scanner(file);

        nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

        attrType = new AttrType[nColumns];


        String columnMetaData[];
        String attribute;
        attrNames = new String[nColumns];

        int stringColumns = 0;
        for (int i = 0; i < attrType.length; i++) {
            columnMetaData = sc.nextLine().trim().split(",");
            attribute = columnMetaData[1];
            attrNames[i] = columnMetaData[0];

            if (attribute.equals("INT")) {
                attrType[i] = new AttrType(AttrType.attrInteger);
            } else {
                attrType[i] = new AttrType(AttrType.attrString);
                stringColumns++;
            }
        }

        attrSizes = new short[stringColumns];
        for (int i = 0; i < stringColumns; i++) {
            attrSizes[i] = attrStringSize;
        }

        for (int i = 0; i < attrType.length; i++) {
            metaTuple.setStrFld(1, attrNames[i]);
            metaTuple.setIntFld(2, attrType[i].attrType);
            if (attrType[i].attrType == AttrType.attrString) {
                metaTuple.setIntFld(3, attrStringSize);
            } else {
                metaTuple.setIntFld(3, 0);
            }

            try {
                rid = f.insertRecord(metaTuple.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        projlist = new FldSpec[nColumns];

        for (int i = 0; i < nColumns; i++) {
            projlist[i] = new FldSpec(rel, i + 1);
        }
    }

    private void setTableMeta(String tableName, AttrType[] attrT, short[] attrS, String[] attrN) throws IOException, FieldNumberOutOfBoundException {
        Heapfile filehf = null;
        try {
            filehf = new Heapfile(tableName + METAFILE_POSTFIX);
            filehf.deleteFile();
            filehf = new Heapfile(tableName + METAFILE_POSTFIX);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < attrT.length; i++) {
            metaTuple.setStrFld(1, attrN[i]);
            metaTuple.setIntFld(2, attrT[i].attrType);
            if (attrT[i].attrType == AttrType.attrString) {
                metaTuple.setIntFld(3, attrStringSize);
            } else {
                metaTuple.setIntFld(3, 0);
            }

            try {
                rid = filehf.insertRecord(metaTuple.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
    }

    public void getTableAttrsAndType(String tableName) {
        // Read data and construct tuples
        FileScan fscan = null;

        FldSpec[] projections = new FldSpec[metaAttrTypes.length];

        for (int i = 0; i < metaAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        nColumns = 0;
        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int stringColumns = 0;
        while (t != null) {
            try {
                if (t.getIntFld(2) == AttrType.attrString) {
                    stringColumns++;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            nColumns++;

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        attrType = new AttrType[nColumns];
        attrSizes = new short[stringColumns];
        attrNames = new String[nColumns];
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int i = 0, j = 0;
        while (t != null) {
            try {
                attrNames[i] = t.getStrFld(1);
                attrType[i] = new AttrType(t.getIntFld(2));
                if (t.getIntFld(2) == AttrType.attrString) {
                    attrSizes[j] = (short) t.getIntFld(3);
                    j++;
                }

            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            i++;
            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        projlist = new FldSpec[nColumns];

        for (i = 0; i < nColumns; i++) {
            projlist[i] = new FldSpec(rel, i + 1);
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public String createTempHeapFileForSkyline(String tableName) {
        Tuple t;
        int indexTypeIfExists = findIfIndexExists(tableName, -1);

        Heapfile tempHF = null;
        String tempHfName = null;

        if (status != OK) {
            return null;
        }
        rid = new RID();
        ArrayList<Tuple> allRows = new ArrayList<Tuple>();
        try {
            if (indexTypeIfExists == NO_INDEX) {
                try {
                    f = new Heapfile(tableName);
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Could not create heap file\n");
                    e.printStackTrace();
                }

                Scan scan = null;
                RID rid = new RID();

                try {
                    scan = f.openScan();
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Error opening scan\n");
                    e.printStackTrace();
                }

                t = new Tuple();

                try {
                    t = scan.getNext(rid);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                while (t != null) {
                    try {
                        //                        tempHF.insertRecord(t.returnTupleByteArray());
                        t.setHdr((short) nColumns, attrType, attrSizes);
                        allRows.add(t);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    try {
                        t =scan.getNext(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                // clean up
                try {
                    scan.closescan();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            } else if (indexTypeIfExists == CLUSTERED_BTREE) {
                bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                BTClusteredFileScan scan = null;
                try {
                    scan = bTreeClusteredFile.new_scan(null, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeyNotMatchException e) {
                    e.printStackTrace();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                }

                KeyDataEntry data = null;
                data = scan.get_next(rid);

                while (data != null) {
                    if (data != null) {
                        try {
                            t = ((Tuple) ((ClusteredLeafData) data.data).getData());
//                            tempHF.insertRecord(t.returnTupleByteArray());
                            allRows.add(t);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    try {
                        data = scan.get_next(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeClusteredFile.close();

            } else if (indexTypeIfExists == CLUSTERED_HASH) {
                hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                ClusteredHashFileScan fscan = null;
                fscan = hashFile.newScan(null, null);
                t = null;
                t = fscan.getNextTuple(rid);

                while (t != null) {
                    try {
//                        tempHF.insertRecord(t.returnTupleByteArray());
                        allRows.add(t);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    try {
                        t = fscan.getNextTuple(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                hashFile.close();
            }
//            sysDef.JavabaseBM.flushPages();
            try {
                tempHfName = Heapfile.getRandomHFName();
                tempHF = new Heapfile(tempHfName);
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not create heap file\n");
                e.printStackTrace();
            }

            for (int i=0; i<allRows.size();i++){
                tempHF.insertRecord(allRows.get(i).returnTupleByteArray());
            }

            return tempHfName;
        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to open heapfile for skyline!");
            e.printStackTrace();
        }
        return null;
    }

    public void getSecondTableAttrsAndType(String tableName) {
        // Read data and construct tuples
        FileScan fscan = null;

        FldSpec[] projections = new FldSpec[metaAttrTypes.length];

        for (int i = 0; i < metaAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        nColumns2 = 0;
        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int stringColumns = 0;
        while (t != null) {
            try {
                if (t.getIntFld(2) == AttrType.attrString) {
                    stringColumns++;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            nColumns2++;

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        attrType2 = new AttrType[nColumns2];
        attrSizes2 = new short[stringColumns];
        attrNames2 = new String[nColumns2];
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int i = 0, j = 0;
        while (t != null) {
            try {
                attrNames2[i] = t.getStrFld(1);
                attrType2[i] = new AttrType(t.getIntFld(2));
                if (t.getIntFld(2) == AttrType.attrString) {
                    attrSizes2[j] = (short) t.getIntFld(3);
                    j++;
                }

            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            i++;
            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        projlist2 = new FldSpec[nColumns2];

        for (i = 0; i < nColumns2; i++) {
            projlist2[i] = new FldSpec(rel, i + 1);
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            QueryInterface qi = new QueryInterface();
            qi.runTests();
        } catch (Exception e) {
            System.err.println("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }


}

/**
 * To get the integer off the command line
 */
class GetStuff {
    GetStuff() {
    }

    public static int getChoice() {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        int choice = -1;

        try {
            choice = Integer.parseInt(in.readLine());
        } catch (NumberFormatException e) {
            return -1;
        } catch (IOException e) {
            return -1;
        }

        return choice;
    }

    public static String getStringChoice() {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String choice;

        try {
            choice = in.readLine();
        } catch (NumberFormatException e) {
            return "Error";
        } catch (IOException e) {
            return "Error";
        }

        return choice;
    }

    public static void getReturn() {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        try {
            String ret = in.readLine();
        } catch (IOException e) {
        }
    }


}

class IndexDesc {
    public int indexType;
    public int attrIndex;

    public IndexDesc(int indexType, int attrIndex) {
        this.attrIndex = attrIndex;
        this.indexType = indexType;
    }
}

class RidTuplePair {
    public RID rid;
    public Tuple tuple;

//    public RidTuplePair(RID rid, Tuple tuple) {
//        this.rid = rid;
//        this.tuple = tuple;
//    }
}