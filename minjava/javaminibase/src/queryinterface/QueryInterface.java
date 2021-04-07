package queryinterface;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.*;
import tests.TestDriver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class QueryInterface extends TestDriver implements GlobalConst{

    protected String dbpath;
    protected String logpath;
    protected String dbName;

    private static RID rid;
    private static Heapfile  f = null;
    private static boolean status = OK;
    private static int nColumns;
    private static AttrType[] attrType;
    private static short[] attrSizes;
    private static short tSize = 34;
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private SystemDefs sysDef;

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
        System.out.println("[13]  Reset Database");
        System.out.println("[14]  Set n_page = 5");
        System.out.println("[15]  Set n_page = 10");
        System.out.println("[16]  Set n_page = <your_wish>");
        System.out.println("\n[0]  Quit");
        System.out.print("Enter your choice :");
    }

    /**
     * QueryInterface Constructor
     */
    public QueryInterface(){
        super("main");
    }

    public boolean runTests () {
        boolean _pass = runAllTests();

        //Clean up again
        cleanDB();

        return _pass;
    }

    protected String testName () {
        return "Query Interface Driver";
    }

    public void cleanDB(){
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

        System.out.println("Successfully deleted database "+dbName.toUpperCase());
    }

    private void closeDB(){
        try {
            sysDef.JavabaseDB.closeDB();
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        System.out.println("Successfully closed database "+dbName.toUpperCase());
    }

    public void openDB(String nameRoot){
        dbName = nameRoot;
        dbpath = "/tmp/"+nameRoot+"-"+System.getProperty("user.name")+".minibase-db";
        logpath = "/tmp/"+nameRoot+"-"+System.getProperty("user.name")+".minibase-log";
        SystemDefs sysdef = new SystemDefs(dbpath,50000, 40000,"Clock");
        System.out.println("Successfully opened database "+dbName.toUpperCase());
    }

    private void createTable(String fileName) throws IOException, InvalidTupleSizeException {

        // Create the heap file object
        try {
            f = new Heapfile(fileName+".in");
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
            File file = new File("../../data/"+fileName + ".txt");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim());

            attrType = new AttrType[nColumns];
            attrSizes = new short[nColumns];

            String columnMetaData;
            String attribute;
            for(int i=0; i<attrType.length; i++){
                columnMetaData = sc.nextLine().trim();
                attribute = columnMetaData.split("\\s+")[1];
                if(attribute == "INT"){
                    attrType[i] = new AttrType(AttrType.attrInteger);
                }else{
                    attrType[i] = new AttrType(AttrType.attrString);
                }
                attrSizes[i] = 32;
            }


            projlist = new FldSpec[nColumns];

            for(int i = 0; i< nColumns; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns,attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: "+size);
            tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size

                String[] row = sc.nextLine().trim().split("\\s+");

                for(int i=0; i<row.length; i++) {
                    try {
                        if(attrType[i].toInt() == AttrType.attrInteger){
                            tuple1.setIntFld(i+1, Integer.parseInt(row[i]));
                        }else{
                            tuple1.setStrFld(i+1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    rid = f.insertRecord(tuple1.returnTupleByteArray());
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                System.out.println("New table created "+fileName.toUpperCase());
                System.out.println("Record count: "+f.getRecCnt());
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            } catch (HFDiskMgrException e) {
                e.printStackTrace();
            } catch (HFBufMgrException e) {
                e.printStackTrace();
            }
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
    WrongPermat
    {

        try {
            f = new Heapfile(tableName+".in");
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

            File file = new File("../../data/"+tableName + ".txt");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim());

            attrType = new AttrType[nColumns];
            attrSizes = new short[nColumns];

            String columnMetaData;
            String attribute;
            for(int i=0; i<attrType.length; i++){
                columnMetaData = sc.nextLine().trim();
                attribute = columnMetaData.split("\\s+")[1];
                if(attribute == "INT"){
                    attrType[i] = new AttrType(AttrType.attrInteger);
                }else{
                    attrType[i] = new AttrType(AttrType.attrString);
                }
                attrSizes[i] = 32;
            }

            projlist = new FldSpec[nColumns];

            for(int i = 0; i< nColumns; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            FileScan fscan = null;

            try {
                fscan = new FileScan(tableName+".in", attrType, attrSizes, (short) nColumns, nColumns, projlist, null);
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


            System.out.println("Record count: "+count);

            // clean up
            try {
                fscan.close();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
    }

    private void insert_data(String tableName, String filename ) throws IOException
    {

        try {
            f = new Heapfile(tableName+".in");
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

            File file = new File("../../data/"+filename + ".txt");
            File fileTable = new File("../../data/"+tableName + ".txt");
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

            for(int i=0; i<attrType.length; i++){
                columnMetaData = sc.nextLine().trim();
                columnMetaDataTable = scTable.nextLine().trim();
                attribute = columnMetaData.split("\\s+")[1];
                attributeTable = columnMetaDataTable.split("\\s+")[1];

                if(!attribute.equals(attributeTable)){
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }

                if(attribute == "INT"){
                    attrType[i] = new AttrType(AttrType.attrInteger);
                }else{
                    attrType[i] = new AttrType(AttrType.attrString);
                }
                attrSizes[i] = 32;
            }

            projlist = new FldSpec[nColumns];

            for(int i = 0; i< nColumns; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns,attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: "+size);
            tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            Integer count = 0 ;
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size

                String[] row = sc.nextLine().trim().split("\\s+");

                for(int i=0; i<row.length; i++) {
                    try {
                        if(attrType[i].toInt() == AttrType.attrInteger){
                            tuple1.setIntFld(i+1, Integer.parseInt(row[i]));
                        }else{
                            tuple1.setStrFld(i+1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    rid = f.insertRecord(tuple1.returnTupleByteArray());
                    count++;
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }


            try {
                System.out.println("New records inserted: "+count);
                System.out.println("Total records in table: "+f.getRecCnt());
                System.out.println();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    private void delete_data(String tableName, String filename ) throws IOException
    {

        try {
            f = new Heapfile(tableName+".in");
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

            File file = new File("../../data/"+filename + ".txt");
            File fileTable = new File("../../data/"+tableName + ".txt");
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

            for(int i=0; i<attrType.length; i++){
                columnMetaData = sc.nextLine().trim();
                columnMetaDataTable = scTable.nextLine().trim();
                attribute = columnMetaData.split("\\s+")[1];
                attributeTable = columnMetaDataTable.split("\\s+")[1];

                if(!attribute.equals(attributeTable)){
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }

                if(attribute == "INT"){
                    attrType[i] = new AttrType(AttrType.attrInteger);
                }else{
                    attrType[i] = new AttrType(AttrType.attrString);
                }
                attrSizes[i] = 32;
            }

            projlist = new FldSpec[nColumns];

            for(int i = 0; i< nColumns; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns,attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: "+size);
            tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            Scan scan = null;
            RID rid = new RID();

            try {
                scan = f.openScan();
            }
            catch (Exception e) {
                status = FAIL;
                System.err.println ("*** Error opening scan\n");
                e.printStackTrace();
            }

            Tuple t = null;
            try {
                t = scan.getNext(rid);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            Integer count = 0 ;
            Boolean match, removed;
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split("\\s+");
                for(int i=0; i<row.length; i++) {
                    try {
                        if(attrType[i].toInt() == AttrType.attrInteger){
                            tuple1.setIntFld(i+1, Integer.parseInt(row[i]));
                        }else{
                            tuple1.setStrFld(i+1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                Boolean result;
                while (t != null) {
                    match = true;

                        try {
                            result = TupleUtils.Equal(tuple1,t,attrType,row.length);
                            if(result){
                                match = false;
                            }
                        }
                        catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                            return;
                        }


                    if(match) {
                        try {
                            removed = f.deleteRecord(rid);
                            if(removed){
                                count++;
                                System.out.print("Successfully deleted: ");
                            }else{
                                System.out.print("Failed to remove: ");
                            }
                            t.print(attrType);

                        }
                        catch (Exception e) {
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
                System.out.println("Records removed: "+count);
                System.out.println("Total records in table: "+f.getRecCnt());
                System.out.println();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    protected boolean runAllTests (){
        int choice=1;
        String tname,fname;

        System.out.println();
        while(choice!=0) {
            menuInterface();

            try{
                choice= GetStuff.getChoice();

                switch(choice) {

                    case 1:
                        System.out.print("Enter Database Name: ");
                        String dbname = GetStuff.getStringChoice();
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
                        System.out.print("Enter Filename: ");
                        fname = GetStuff.getStringChoice();
                        System.out.print("Enter Tablename: ");
                        tname = GetStuff.getStringChoice();
                        System.out.println();
                        insert_data(tname,fname);
                        break;

                    case 6:
                        System.out.print("Enter Filename: ");
                        fname = GetStuff.getStringChoice();
                        System.out.print("Enter Tablename: ");
                        tname = GetStuff.getStringChoice();
                        System.out.println();
                        delete_data(tname,fname);
                        break;

                    case 7:
                        System.out.print("Enter Tablename: ");
                        tname = GetStuff.getStringChoice();
                        System.out.println();
                        printTable(tname);
                        break;

                    case 8:
                        break;

                    case 9:
                        break;
                    case 10:
                        break;

                    case 11:
                        break;

                    case 12:

                        break;

                    case 13:
                        break;

                    case 0:
                        break;
                }
            }
            catch(Exception e) {
                e.printStackTrace();
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("       !!               Something is wrong              !!");
                System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            }
        }
        return true;
    }



    public static void main(String [] args) {
        try{
            QueryInterface qi = new QueryInterface();
            qi.runTests();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
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

    public static String getStringChoice () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        String choice ;

        try {
            choice = in.readLine();
        }
        catch (NumberFormatException e) {
            return "Error";
        }
        catch (IOException e) {
            return "Error";
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