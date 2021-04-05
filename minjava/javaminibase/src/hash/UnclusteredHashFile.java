package hash;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.IOException;

public class UnclusteredHashFile extends HashFile {

    public UnclusteredHashFile(String filename) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        super(filename,false);
    }

    public UnclusteredHashFile(String filename, int keyType, int keySize, int targetUtilization) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        super(filename, keyType, keySize, targetUtilization,false);
    }

    public void insertRecord(KeyClass key, RID rid) throws IOException, ConstructPageException, InvalidSlotNumberException {
        super.insertRecord(key, new UnclusteredHashRecord(key, rid));
    }

    public void deleteRecord(KeyClass key, RID rid) throws IOException, ConstructPageException, InvalidSlotNumberException, InvalidTypeException, UnknowAttrType, TupleUtilsException, InvalidTupleSizeException {
        super.deleteRecord(key, new UnclusteredHashRecord(key, rid));
    }
}
