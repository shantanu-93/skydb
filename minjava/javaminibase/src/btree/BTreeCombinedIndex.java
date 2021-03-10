package btree;

import java.io.*;
import java.security.Key;
import java.util.*;


import java.lang.*;

import heap.*;
import bufmgr.*;
import global.*;
import btree.*;

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

public class BTreeCombinedIndex{
    BTreeFile file;
    int id = 0;
    public int prefix = 0;
    
    public BTreeCombinedIndex(){
    }
    
    float floatKey(double[] values, int[] pref_list, int pref_list_length) {
		float sum = 0.0f;
        int j = 0;
		for(int i = 0; i < values.length; i++) {
			if(pref_list[j] == i && j < pref_list_length) {
				sum += (float) values[i];
                j++;
			}
		}
		return sum;
	}

    double[][] readData(String filePath) throws FileNotFoundException {
        File dfile = new File(filePath);
        Scanner sc = new Scanner(dfile);
        int COLS = sc.nextInt();
        List<double[]> records = new ArrayList<double[]>();

        while (sc.hasNextLine()) {
            double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                    .split("\\s+"))
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new))
                    .mapToDouble(Double::parseDouble)
                    .toArray();
            if(doubleArray.length != 0){
                records.add(doubleArray);
            }
        }
        double [][] ret = new double[records.size()][];
        ret = records.toArray(ret);
        return ret;
    }

    public IndexFile combinedIndex(String filePath, int[] pref_list, int pref_list_length) throws Exception {

        double[][] data = readData(filePath);
        String filename = "AAA";
        int col = data[0].length;

        int keyType = AttrType.attrReal;
        int keySize = 4;

        Heapfile heapfile = new Heapfile("heap_" + filename);

        file = new BTreeFile(filename, keyType, keySize, 1);

        AttrType [] Stypes = new AttrType[col];

        for(int i=0; i < col; i++){
            Stypes[i] = new AttrType(AttrType.attrReal);
        }

        Tuple tuple = new Tuple();
        short [] Ssizes = null;

        tuple.setHdr((short) col,Stypes, Ssizes);
        int size = tuple.size();
        

        tuple = new Tuple(size);
        tuple.setHdr((short) col, Stypes, Ssizes);

        RID rid;
        
        float fkey;
        KeyClass ffkey;
        
        for(double[] value: data){
            
            fkey = floatKey(value, pref_list, pref_list_length);
            ffkey = new FloatKey(fkey);

            for(int i=0; i<value.length; i++) {
                tuple.setFloFld(i+1, (float) value[i]);
            }
            
            rid = heapfile.insertRecord(tuple.returnTupleByteArray());
                       
            file.insert(ffkey, rid);
        }
        return file;
    }
}
