package btree;

import global.RID;
import heap.Tuple;

/**
 * IndexData: It extends the DataClass.
 * It defines the data "rid" for leaf node in B++ tree.
 */
public class ClusteredLeafData extends DataClass {
    private Tuple data;

//  public String toString() {
//     String s;
//     s="[ "+ (new Integer(myRid.pageNo.pid)).toString() +" "
//              + (new Integer(myRid.slotNo)).toString() + " ]";
//     return s;
//  }

    ClusteredLeafData(Tuple data) {
        this.data = data;
    }

    /**
     * get a copy of the rid
     *
     * @return the reference of the copy
     */
    public Tuple getData() {
        return data;
    }

    ;

    /**
     * set the rid
     */
    public void setData(Tuple data) {
        this.data = data;
    }
}   
