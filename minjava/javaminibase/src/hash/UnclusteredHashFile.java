package hash;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import heap.InvalidSlotNumberException;

import java.io.IOException;

public class UnclusteredHashFile extends HashFile {

    public UnclusteredHashFile(String filename) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        super(filename, 0, false);
    }

    public UnclusteredHashFile(String filename, int targetUtilization) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        super(filename, targetUtilization,false);
    }


}
