package tests;

import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;

import btree.AddFileEntryException;
import btree.BTClusteredFileScan;
import btree.BTreeClusteredFile;
import btree.ConstructPageException;
import btree.FloatKey;
import btree.GetFileEntryException;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import diskmgr.PCounter;
import btree.ClusteredLeafData;

class TopK_NRAJoinDriver extends TestDriver
        implements GlobalConst {

    private static String data1[] = {
            "raghu", "xbao", "cychan", "leela", "ketola", "soma", "ulloa",
            "dhanoa", "dsilva", "kurniawa", "dissoswa", "waic", "susanc", "kinc",
            "marc", "scottc", "yuc", "ireland", "rathgebe", "joyce", "daode",
            "yuvadee", "he", "huxtable", "muerle", "flechtne", "thiodore", "jhowe",
            "frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch",
            "binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
            "chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel",
            "newell", "schiesl", "neuman", "heitzman", "wan", "gunawan", "djensen",
            "juei-wen", "josephin", "harimin", "xin", "zmudzin", "feldmann",
            "joon", "wawrzon", "yi-chun", "wenchao", "seo", "karsono", "dwiyono",
            "ginther", "keeler", "peter", "lukas", "edwards", "mirwais", "schleis",
            "haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
            "honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey",
            "awny", "savoy", "meltz"};

    private static String data2[] = {
            "andyw", "awny", "azat", "barthel", "binh", "bloch", "bradw",
            "chingju", "chui", "chung-pi", "cychan", "dai", "daode", "dhanoa",
            "dissoswa", "djensen", "dsilva", "dwiyono", "edwards", "evgueni",
            "feldmann", "flechtne", "frankief", "ginther", "gray", "guangshu",
            "gunawan", "hai", "handi", "harimin", "haris", "he", "heitzman",
            "honghu", "huxtable", "ireland", "jhowe", "joon", "josephin", "joyce",
            "jsong", "juei-wen", "karsono", "keeler", "ketola", "kinc", "kurniawa",
            "leela", "lukas", "mak", "marc", "markert", "meltz", "meyers",
            "mirwais", "muerle", "muthiah", "neuman", "newell", "peter", "raghu",
            "randal", "rathgebe", "robert", "savoy", "schiesl", "schleis",
            "scottc", "seo", "shi", "shun-kit", "siddiqui", "soma", "sonthi",
            "sungk", "susanc", "tak", "thiodore", "ulloa", "vharvey", "waic",
            "wan", "wawrzon", "wenchao", "wlau", "xbao", "xiaoming", "xin",
            "yi-chun", "yiching", "yuc", "yung", "yuvadee", "zmudzin"};

    private static int NUM_RECORDS = data2.length;
    private static int LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private static int SORTPGNUM = 12;


    public TopK_NRAJoinDriver() {
        super("skylinetest");
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 10000, 10000, "Clock");

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
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.println("\n" + "..." + testName() + " tests ");
        System.out.println(_pass == OK ? "completely successfully" : "failed");
        System.out.println(".\n\n");

        return _pass;
    }

    /*  TopK_NRAJoin(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            FldSpec joinAttr1,
            FldSpec mergeAttr1,
            AttrType[] in2, int len_in2, short[] t2_str_sizes,
            FldSpec joinAttr2,
            FldSpec mergeAttr2,
            java.lang.String relationName1,
            java.lang.String relationName2,
            int k,
            int n_pages
        ) */

    protected boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");

        System.out.println("\n -- Testing TopK_NRAJoin on only real values-- ");

        boolean status = OK;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[0];

        Tuple t = new Tuple();

        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        BTreeClusteredFile file = null;
        
        try {
            file = new BTreeClusteredFile("test1.in", AttrType.attrReal, 4, 1, (short) 2, attrType, attrSize);
        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AddFileEntryException e) {
            e.printStackTrace();
        }

        System.out.println("\n -- Inserting tuples -- ");
        
        ArrayList<float[]> list = new ArrayList<>();

        list.add(new float[]{1.0f,1.0f});
        list.add(new float[]{2.0f,0.8f});
        list.add(new float[]{3.0f,0.5f});
        list.add(new float[]{4.0f,0.3f});
        list.add(new float[]{5.0f,0.1f});

        int num_elements = list.size();

        for (int i = 0; i < num_elements; i++) {

            try {
                t.setFloFld(1, list.get(i)[0]);
                t.setFloFld(2, list.get(i)[1]);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                file.insert(new FloatKey(-list.get(i)[1]), t);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            System.out.println("fld1 = " + list.get(i)[0] + " fld2 = " + list.get(i)[1]);
        }

        // System.out.println("\n -- Scanning BTreeClusteredFile");
        // FloatKey key1 = new FloatKey(-1.0F);
        // FloatKey key2 = new FloatKey(0.0F);
        // BTClusteredFileScan scan = null;
        // try {

        //     scan = file.new_scan(key1, key2);
        // } catch (IOException e) {
        //     e.printStackTrace();
        // } catch (KeyNotMatchException e) {
        //     e.printStackTrace();
        // } catch (IteratorException e) {
        //     e.printStackTrace();
        // } catch (ConstructPageException e) {
        //     e.printStackTrace();
        // } catch (PinPageException e) {
        //     e.printStackTrace();
        // } catch (UnpinPageException e) {
        //     e.printStackTrace();
        // }
        // KeyDataEntry data = null;
        // try {
        //     data = scan.get_next();
        //     if (data != null) {
        //         try {
        //             ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
        //         } catch (IOException e) {
        //             e.printStackTrace();
        //         }
        //     }
        // } catch (ScanIteratorException e) {
        //     e.printStackTrace();
        // }

        // while (data != null) {
        //     try {
        //         data = scan.get_next();
        //         if (data != null) {
        //             try {
        //                 ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
        //             } catch (IOException e) {
        //                 e.printStackTrace();
        //             }
        //         }

        //     } catch (ScanIteratorException e) {
        //         e.printStackTrace();
        //     }

        // }

        try {
            file = new BTreeClusteredFile("test2.in", AttrType.attrReal, 4, 1, (short) 2, attrType, attrSize);
        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AddFileEntryException e) {
            e.printStackTrace();
        }

        System.out.println("\n -- Inserting tuples -- ");
        
        ArrayList<float[]> list2 = new ArrayList<>();

        list2.add(new float[]{1.0f,0.3f});
        list2.add(new float[]{2.0f,0.1f});
        list2.add(new float[]{3.0f,0.7f});
        list2.add(new float[]{4.0f,0.2f});
        list2.add(new float[]{5.0f,0.1f});

        num_elements = list2.size();

        for (int i = 0; i < num_elements; i++) {

            try {
                t.setFloFld(1, list2.get(i)[0]);
                t.setFloFld(2, list2.get(i)[1]);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                file.insert(new FloatKey(-list2.get(i)[1]), t);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            System.out.println("fld1 = " + list2.get(i)[0] + " fld2 = " + list2.get(i)[1]);
        }

        // System.out.println("\n -- Scanning BTreeClusteredFile");
        // try {
        //     scan = file.new_scan(key1, key2);
        // } catch (IOException e) {
        //     e.printStackTrace();
        // } catch (KeyNotMatchException e) {
        //     e.printStackTrace();
        // } catch (IteratorException e) {
        //     e.printStackTrace();
        // } catch (ConstructPageException e) {
        //     e.printStackTrace();
        // } catch (PinPageException e) {
        //     e.printStackTrace();
        // } catch (UnpinPageException e) {
        //     e.printStackTrace();
        // }
        // data = null;
        // try {
        //     data = scan.get_next();
        //     if (data != null) {
        //         try {
        //             ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
        //         } catch (IOException e) {
        //             e.printStackTrace();
        //         }
        //     }
        // } catch (ScanIteratorException e) {
        //     e.printStackTrace();
        // }

        // while (data != null) {
        //     try {
        //         data = scan.get_next();
        //         if (data != null) {
        //             try {
        //                 ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
        //             } catch (IOException e) {
        //                 e.printStackTrace();
        //             }
        //         }

        //     } catch (ScanIteratorException e) {
        //         e.printStackTrace();
        //     }

        // }
        FldSpec[] joinList = new FldSpec[2];
        FldSpec[] mergeList = new FldSpec[2];
        RelSpec rel1= new RelSpec(RelSpec.outer);
        RelSpec rel2 = new RelSpec(RelSpec.innerRel);

        joinList[0] = new FldSpec(rel1, 1);
        joinList[1] = new FldSpec(rel2, 1);
        mergeList[0] = new FldSpec(rel1, 2);
        mergeList[1] = new FldSpec(rel2, 2);

        TopK_NRAJoin topK_NRAJoin = new TopK_NRAJoin(attrType, attrType.length, attrSize, joinList[0], mergeList[0], attrType, attrType.length, attrSize, 
            joinList[1], mergeList[1], "test1.in", "test2.in", 2, 50);
        
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

        return status;
    }


    protected boolean test2() {
        return true;
    }

    protected boolean test3() {
        return true;
    }

    protected boolean test4() {
        return true;
    }

    protected boolean test5() {
        return true;    }

    protected boolean test6() {
        return true;
    }

    protected String testName() {
        return "Top K_NRAJoin";
    }
}

public class TopK_NRAJoinTest {
    public static void main(String argv[]) {
        boolean sortstatus;

        TopK_NRAJoinDriver sortt = new TopK_NRAJoinDriver();

        sortstatus = sortt.runTests();
        
        if (sortstatus != true) {
            System.out.println("Error ocurred during tests");
        } else {
            System.out.println("TopK_NRAJoin tests completed successfully");
        }
    }
}

