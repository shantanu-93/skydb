package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortFirstSky extends Iterator {
        private AttrType      _in1[];
        private   short        in1_len;
        private   Iterator  outer;
        private   int        n_buf_pgs;        // # of buffer pages available.
        private int[] pref_list_cls;
        private int pref_list_length_cls;
        private   Iterator  sort;
        private List<Tuple> inner;
        private short[] t1_str_sizes_cls;
        int maxRecordSize;

        public SortFirstSky(
                         AttrType[] in1,
                         int     len_in1,
                         short[] t1_str_sizes,
                         Iterator am1,
                         java.lang.String relationName,
                         int[] pref_list,
                         int pref_list_length,
                         int n_pages
        )
                throws JoinNewFailed,
                JoinLowMemory,
                SortException,
                TupleUtilsException,
                IOException,
                SortException

        {

                _in1 = new AttrType[in1.length];
                System.arraycopy(in1,0,_in1,0,in1.length);
                in1_len = (short) len_in1;

                outer = am1;

                n_buf_pgs    = n_pages;
                inner = null;

                pref_list_cls = pref_list;
                pref_list_length_cls = pref_list_length;
                t1_str_sizes_cls = t1_str_sizes;

                //Getting the maximum number of records on one page.
                RID id = new RID();
                Heapfile hf = null;
                try {
                        hf = new Heapfile(relationName);
                }
                catch(Exception e) {
                        throw new SortException(e, "Create new heapfile failed.");
                }

                Scan sc = null;
                try{
                        sc = hf.openScan();
                }catch(Exception e){
                        throw new SortException(e, "openScan failed");
                }

                try{
                        sc.getNextAndCountRecords(id);

                }catch(Exception e){
                        throw new SortException(e, "Could not get number of records on page 1");
                }
                System.out.println(sc.getNumberOfRecordsPerOnePage());

                maxRecordSize = sc.getNumberOfRecordsPerOnePage()* n_buf_pgs;

                try {
                        sort = new SortPref(_in1, in1_len, t1_str_sizes, outer, new TupleOrder(TupleOrder.Descending) , pref_list_cls, pref_list_length_cls, n_buf_pgs);
                } catch (Exception e) {
                        e.printStackTrace();
                }

                inner = new ArrayList<>();
        }


        public Tuple get_next()
                throws IOException,
                JoinsException ,
                IndexException,
                InvalidTupleSizeException,
                InvalidTypeException,
                PageNotReadException,
                TupleUtilsException,
                PredEvalException,
                SortException,
                LowMemException,
                UnknowAttrType,
                UnknownKeyTypeException,
                Exception
        {

                Tuple skylineTuple = null;
                Tuple currentOuter = null;

                while (true) {
                        currentOuter = sort.get_next();

                        if (currentOuter == null) {
                                break;
                        }

                        boolean dominated = false;

                        for (Tuple innerTuple : inner) {
                                if (TupleUtils.Dominates(innerTuple, _in1, currentOuter, _in1, in1_len, t1_str_sizes_cls, pref_list_cls, pref_list_length_cls)) {
                                        dominated = true;
                                        break;
                                }
                        }

                        if (!dominated) {
                                Tuple temp = new Tuple(currentOuter);
                                if (inner.size() < maxRecordSize) {
                                        inner.add(temp);
                                } else {
                                        throw new LowMemException("SortFirstSky.java: Not enough memory");
                                }

                                skylineTuple = currentOuter;
                                break;
                        }
                }

                return skylineTuple;
        }

        public void close()
                throws JoinsException,
                IOException,
                IndexException, SortException {
                if (!closeFlag) {
                        sort.close();
                        inner.clear();
                        closeFlag = true;
                }
        }

}
