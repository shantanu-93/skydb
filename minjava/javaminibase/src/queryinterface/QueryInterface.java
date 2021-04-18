package queryinterface;

import btree.*;
import bufmgr.*;
import catalog.attrInfo;
import diskmgr.PCounter;
import global.*;
import heap.*;
import iterator.*;
import tests.TestDriver;

import java.io.*;
import java.util.Arrays;
import java.util.Scanner;

public class QueryInterface extends TestDriver implements GlobalConst {

    protected String dbpath;
    protected String logpath;
    protected String dbName;

    private static RID rid;
    private static int _n_pages;
    private static Heapfile f = null;
    private static boolean status = OK;
    private static int nColumns;
    private static AttrType[] attrType;
    private static short[] attrSizes;
    private static short tSize = 34;
    private static int[] pref_list;
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private SystemDefs sysDef;
    private static boolean indexesCreated;
    private boolean dbClosed = true;

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
        boolean _pass = runAllTests();

        //Clean up again
        //cleanDB();
        if (!dbClosed) {
            closeDB();
        }

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");

        return _pass;
    }

    protected String testName() {
        return "Query Interface Driver";
    }

    protected boolean runAllTests() {
        int choice = 1;
        String tname, fname, dbname;

        System.out.println();
        while (choice != 0) {
            menuInterface();

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
                            openDB(dbname);
                            break;

                        case 2:
                            closeDB();
                            break;

                        case 3:
                            System.out.print("Enter Filename: ");
                            fname = GetStuff.getStringChoice();
                            System.out.println();
                            createTable(fname);
                            break;

                        case 4:

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
                                createTableNRA(fileName1, jAttr1, mAttr1);
                                createTableNRA(fileName2, jAttr2, mAttr2);

                                scanBTCluster(fileName1, attrType, attrSizes);
                                scanBTCluster(fileName2, attrType, attrSizes);

                                FldSpec[] joinList = new FldSpec[2];
                                FldSpec[] mergeList = new FldSpec[2];
                                
                                joinList[0] = new FldSpec(rel, jAttr1);
                                joinList[1] = new FldSpec(rel, jAttr2);
                                mergeList[0] = new FldSpec(rel, mAttr1);
                                mergeList[1] = new FldSpec(rel, mAttr2);

                                TopK_NRAJoin topK_NRAJoin = new TopK_NRAJoin(attrType, attrType.length, attrSizes, joinList[0], mergeList[0], 
                                attrType, attrType.length, attrSizes, joinList[1], mergeList[1], fileName1, fileName2, k, n_pages);

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
            setAttrDesc(tname);
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
                switch (choice) {
                    case 1:
                        // call nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runNestedLoopSky(tname);
                        break;

                    case 2:
                        // call block nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBNLSky(tname);
                        break;

                    case 3:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runSortFirstSky(tname);
                        break;

                    case 4:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBtreeSky(tname);
                        break;

                    case 5:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBTreeSortedSky(tname);
                        break;

                    case 0:
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

    private void closeDB() {
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

    public void openDB(String nameRoot) {
        dbName = nameRoot;
        dbpath = "/tmp/" + nameRoot + ".minibase-db";
        logpath = "/tmp/" + nameRoot + ".minibase-log";
        File f = new File(dbpath);

        if(f.exists() && !f.isDirectory()) {
            sysDef = new ExtendedSystemDefs(dbpath, 0, 40000, "Clock");
            System.out.println("Successfully opened database " + dbName.toUpperCase());
        }else{
            sysDef = new ExtendedSystemDefs(
                    dbpath, 50000, 40000, "Clock");
            System.out.println("Successfully created database " + dbName.toUpperCase());
        }

    }

    private void createTable(String fileName) throws IOException, InvalidTupleSizeException {

        // Create the heap file object
        try {
            f = new Heapfile(fileName);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if (status == OK) {

            // Read data and construct tuples
            File file = new File("../../data/" + fileName + ".txt");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim());

            attrType = new AttrType[nColumns];
            attrInfo[] ai = new attrInfo[nColumns] ;
            attrSizes = new short[nColumns];
            String[] columnMetaData;
            String attribute;
            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim().split("\\s+");
                attribute = columnMetaData[1];
                if (attribute.equals("INT")) {
                    attrType[i] = new AttrType(AttrType.attrInteger);
                } else {
                    attrType[i] = new AttrType(AttrType.attrString);
                    attrSizes[i] = 32;
                }

                ai[i] = new attrInfo();
                ai[i].attrName = columnMetaData[0];
                ai[i].attrType = attrType[i];
                ai[i].attrLen = 32;
            }

//            try {
//                ExtendedSystemDefs.MINIBASE_CATALOGPTR.createRel(fileName,nColumns,ai);
//            } catch (Exception e) {
//                System.err.println("*** error in creating relation ***");
//                status = FAIL;
//                e.printStackTrace();
//            }

            projlist = new FldSpec[nColumns];

            for (int i = 0; i < nColumns; i++) {
                projlist[i] = new FldSpec(rel, i + 1);
                ;
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
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split("\\s+");

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
                    rid = f.insertRecord(tuple1.returnTupleByteArray());
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                System.out.println("New table created " + fileName.toUpperCase());
                System.out.println("Record count: " + f.getRecCnt());
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            } catch (HFDiskMgrException e) {
                e.printStackTrace();
            } catch (HFBufMgrException e) {
                e.printStackTrace();
            }
        }
    }

    private void setAttrDesc(String tableName) throws FileNotFoundException {
        File file = new File("../../../data/" + tableName + ".txt");
        Scanner sc = new Scanner(file);

        nColumns = Integer.valueOf(sc.nextLine().trim());

        attrType = new AttrType[nColumns];
        attrSizes = new short[nColumns];

        String columnMetaData;
        String attribute;
        for (int i = 0; i < attrType.length; i++) {
            columnMetaData = sc.nextLine().trim();
            attribute = columnMetaData.split("\\s+")[1];
            if (attribute.equals("INT")) {
                attrType[i] = new AttrType(AttrType.attrInteger);
            } else {
                attrType[i] = new AttrType(AttrType.attrString);
                attrSizes[i] = 32;
            }
//            System.out.println(attrType[i]);
        }
    }

    private void printTable(String tableName) throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat {
        status = OK;
        try {
            f = new Heapfile(tableName);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }
//        AttrType[] at;
//        short[] as;
//        AttrDesc[] ad = new AttrDesc[2];
//
//        try {
//            ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(tableName,0,ad);
//        } catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }

        if (status == OK) {
            File file = new File("../../data/" + tableName + ".txt");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim());

            attrType = new AttrType[nColumns];
            attrSizes = new short[nColumns];

            String columnMetaData;
            String attribute;
            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim();
                attribute = columnMetaData.split("\\s+")[1];
                if (attribute.equals("INT")) {
                    attrType[i] = new AttrType(AttrType.attrInteger);
                } else {
                    attrType[i] = new AttrType(AttrType.attrString);
                    attrSizes[i] = 32;
                }
            }

            projlist = new FldSpec[nColumns];

            for (int i = 0; i < nColumns; i++) {
                projlist[i] = new FldSpec(rel, i + 1);
                ;
            }

            FileScan fscan = null;

            try {
                fscan = new FileScan(tableName, attrType, attrSizes, (short) nColumns, nColumns, projlist, null);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            Integer count = 0;
            Tuple t = null;
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
        }
    }

    private void insert_data(String tableName, String filename) throws IOException {

        try {
            f = new Heapfile(tableName);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if (status == OK) {

            File file = new File("../../../data/" + filename + ".txt");
            File fileTable = new File("../../../data/" + tableName + ".txt");
            Scanner sc = new Scanner(file);
            Scanner scTable = new Scanner(fileTable);

            nColumns = Integer.valueOf(sc.nextLine().trim());
            scTable.nextLine();
            attrType = new AttrType[nColumns];
            attrSizes = new short[nColumns];

            String columnMetaData;
            String columnMetaDataTable;
            String attribute;
            String attributeTable;

            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim();
                columnMetaDataTable = scTable.nextLine().trim();
                attribute = columnMetaData.split("\\s+")[1];
                attributeTable = columnMetaDataTable.split("\\s+")[1];

                if (!attribute.equals(attributeTable)) {
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }

                if (attribute.equals("INT")) {
                    attrType[i] = new AttrType(AttrType.attrInteger);
                } else {
                    attrType[i] = new AttrType(AttrType.attrString);
                }
                attrSizes[i] = 32;
            }

            projlist = new FldSpec[nColumns];

            for (int i = 0; i < nColumns; i++) {
                projlist[i] = new FldSpec(rel, i + 1);
                ;
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

            Integer count = 0;
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size

                String[] row = sc.nextLine().trim().split("\\s+");

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

                try {
                    rid = f.insertRecord(tuple1.returnTupleByteArray());
                    count++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }


            try {
                System.out.println("New records inserted: " + count);
                System.out.println("Total records in table: " + f.getRecCnt());
                System.out.println();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    private void delete_data(String tableName, String filename) throws IOException {

        try {
            f = new Heapfile(tableName);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if (status == OK) {

            File file = new File("../../../data/" + filename + ".txt");
            File fileTable = new File("../../../data/" + tableName + ".txt");
            Scanner sc = new Scanner(file);
            Scanner scTable = new Scanner(fileTable);

            nColumns = Integer.valueOf(sc.nextLine().trim());
            scTable.nextLine();
            attrType = new AttrType[nColumns];
            attrSizes = new short[nColumns];

            String columnMetaData;
            String columnMetaDataTable;
            String attribute;
            String attributeTable;

            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim();
                columnMetaDataTable = scTable.nextLine().trim();
                attribute = columnMetaData.split("\\s+")[1];
                attributeTable = columnMetaDataTable.split("\\s+")[1];

                if (!attribute.equals(attributeTable)) {
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }

                if (attribute.equals("INT")) {
                    attrType[i] = new AttrType(AttrType.attrInteger);
                } else {
                    attrType[i] = new AttrType(AttrType.attrString);
                }
                attrSizes[i] = 32;
            }

            projlist = new FldSpec[nColumns];

            for (int i = 0; i < nColumns; i++) {
                projlist[i] = new FldSpec(rel, i + 1);
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



            Integer count = 0;
            Boolean match, removed;
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split("\\s+");
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

                Boolean result;
                Scan scan = null;
                RID rid = new RID();

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
                    t.print(attrType);
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
                            removed = f.deleteRecord(rid);
                            if (removed) {
                                count++;
                                System.out.print("Successfully deleted: ");
                            } else {
                                System.out.print("Failed to remove: ");
                            }
                            t.print(attrType);
                            break;
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
            }

            try {
                System.out.println("Records removed: " + count);
                System.out.println("Total records in table: " + f.getRecCnt());
                System.out.println();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    private void createTableNRA(String fileName, int jAttr, int mAttr) throws IOException, InvalidTupleSizeException {

        if (status == OK) {

            // Read data and construct tuples
            File file = new File("../../data/" + fileName + ".txt");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim());

            attrType = new AttrType[nColumns];
            attrInfo[] ai = new attrInfo[nColumns] ;
            attrSizes = new short[nColumns];
            String[] columnMetaData;
            String attribute;

            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim().split("\\s+");
                attribute = columnMetaData[1];
                if (attribute.equals("INT")) {
                    attrType[i] = new AttrType(AttrType.attrInteger);
                    attrSizes[i] = 4;
                } else {
                    attrType[i] = new AttrType(AttrType.attrString);
                    attrSizes[i] = 32;
                }

                ai[i] = new attrInfo();
                ai[i].attrName = columnMetaData[0];
                ai[i].attrType = attrType[i];
                ai[i].attrLen = 32;
            }

            projlist = new FldSpec[nColumns];

            for (int i = 0; i < nColumns; i++) {
                projlist[i] = new FldSpec(rel, i + 1);
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

            BTreeClusteredFile bfile = null;

            try {
                bfile = new BTreeClusteredFile(fileName, attrType[mAttr - 1].attrType, attrSizes[mAttr - 1], mAttr, 1, (short) attrType.length, attrType, attrSizes);
            } catch (GetFileEntryException e) {
                e.printStackTrace();
            } catch (ConstructPageException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (AddFileEntryException e) {
                e.printStackTrace();
            }

            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split("\\s+");

                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            if(i == mAttr - 1)
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
                    if(attrType[mAttr - 1].attrType == AttrType.attrString)
                        bfile.insert(new StringKey(row[mAttr - 1]), tuple1);
                    else
                        bfile.insert(new IntegerKey(-Integer.valueOf(row[mAttr - 1])), tuple1);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            System.out.println("Table "+fileName+" created");
        }
    }

    public void scanBTCluster(String relationName1, AttrType[] in1, short[] t1_str_sizes){
        BTClusteredFileScan scan = null;
        BTreeClusteredFile file = null;
        try {
            file = new BTreeClusteredFile(relationName1, (short) attrType.length, in1, t1_str_sizes);
        } catch (GetFileEntryException e1) {
            e1.printStackTrace();
        } catch (PinPageException e1) {
            e1.printStackTrace();
        } catch (ConstructPageException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        try {
            scan = file.new_scan(new IntegerKey(-100000), new IntegerKey(0));
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
        try {
            data = scan.get_next();
            if (data != null) {
                try {
                    ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (ScanIteratorException e) {
            e.printStackTrace();
        }

        while (data != null) {
            try {
                data = scan.get_next();
                if (data != null) {
                    try {
                        ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } catch (ScanIteratorException e) {
                e.printStackTrace();
            }

        }
        

    }

    public static void runNestedLoopSky(String hf) throws PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanNested = initialiseFileScan(hf);
        getTableAttrsAndType(hf);
        NestedLoopsSky nested = null;
        try {
            nested = new NestedLoopsSky(attrType, attrType.length, attrSizes, fscanNested, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(nested);

        try {
            nested.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runBNLSky(String hf) throws PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanBlock = initialiseFileScan(hf);
        getTableAttrsAndType(hf);
        Iterator block = null;
        try {
            block = new BlockNestedLoopsSky(attrType, attrType.length, attrSizes, fscanBlock, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(block);

        try {
            block.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runSortFirstSky(String hf) throws PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscan = initialiseFileScan(hf);
        getTableAttrsAndType(hf);
        Iterator sort = null;
        try {
            sort = new SortFirstSky(attrType, attrType.length, attrSizes, fscan, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(sort);

        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    private void runBtreeSky(String hf) throws Exception {
        System.out.println("Running BTreeSky");
        System.out.println("DataFile: " + hf);
        System.out.println("Preference list: " + Arrays.toString(pref_list));
        System.out.println("Number of pages: " + _n_pages);
        System.out.println("Pref list length: " + pref_list.length);
        getTableAttrsAndType(hf);
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
        btreesky.findBTreeSky();

        System.out.println("BTreeSky Complete\n");
    }

    public void runBTreeSortedSky(String hf) {
        try {
            BTreeCombinedIndex obj = new BTreeCombinedIndex();
            IndexFile indexFile = obj.combinedIndex(hf, attrType, attrSizes, pref_list, pref_list.length);
            System.out.println("Index created!");
            SystemDefs.JavabaseBM.flushPages();

            System.out.println("CombinedBTreeIndex scanning");
            String fileName = BTreeCombinedIndex.random_string1;

            BTreeSortedSky btree = new BTreeSortedSky(attrType, attrType.length, attrSizes, null, fileName, pref_list, pref_list.length, indexFile, _n_pages);
            PCounter.initialize();
            btree.computeSkyline();

            System.out.println("BTreeSortSky Complete");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getNextAndPrintAllSkyLine(Iterator iter) {
// this needs to be before the fn call since call to any algo 1,2,3 from 4,5 reinitializes counter
//        PCounter.initialize();

        int count = -1;
        Tuple tuple1 = null;
        System.out.println(attrType[0].attrType);
        System.out.println(attrType[1].attrType);
        System.out.println(attrType[2].attrType);
        System.out.println("\n -- Skyline Objects -- ");
        do {
            try {
                if (tuple1 != null) {
                    tuple1.print(attrType);
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

    public static void getTableAttrsAndType(String fileName) {
        // Read data and construct tuples
        File file = new File("../../../data/" + fileName + ".txt");

        Scanner sc = null;
        try {
            sc = new Scanner(file);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        nColumns = Integer.valueOf(sc.nextLine().trim());

        attrType = new AttrType[nColumns];
        attrSizes = new short[nColumns];

        String columnMetaData;
        String attribute;
        for (int i = 0; i < attrType.length; i++) {
            columnMetaData = sc.nextLine().trim();
            attribute = columnMetaData.split("\\s+")[1];
            if (attribute.equals("INT")) {
                attrType[i] = new AttrType(AttrType.attrInteger);
            } else {
                attrType[i] = new AttrType(AttrType.attrString);
            }
            attrSizes[i] = 32;
        }


        projlist = new FldSpec[nColumns];

        for (int i = 0; i < nColumns; i++) {
            projlist[i] = new FldSpec(rel, i + 1);
            ;
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