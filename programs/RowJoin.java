package programs;

import iterator.*;
import index.*;
import BigT.*;
import bufmgr.*;
import global.*;

/**
 * Rowjoin
 * Joins two tables: Explained details in nestedloopjoin.java
 */

public class RowJoin extends TestDriver{

    @Override
    public void performCmd(String[] words) {
        String leftbt = words[1];
        String rightbt = words[2];
        String colfilter = words[3];
        String outbt = words[4];
        int numbf = Integer.parseInt(words[5]);
        try {
            bigt f = new bigt(leftbt);
            rowJoin(f.openStream(), rightbt, colfilter, outbt, numbf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void rowJoin(Stream leftstream, String rightbt, String colFilter, String outbt, int numbf)
    throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException,
    PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException,
    Exception {
        updateNumbuf(numbf);
        FldSpec[] proj_list = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        bigt oBigt = new bigt(outbt);
        proj_list[0]= new FldSpec(rel, 1);
        proj_list[1]= new FldSpec(rel, 2);
        proj_list[2]= new FldSpec(rel, 3);
        proj_list[3]= new FldSpec(rel, 4);
        NestedLoopsJoins nestedLoopsJoins = new NestedLoopsJoins(colFilter, numbf, leftstream, rightbt, null, null, proj_list, 4);
        while (true) {
            Map m = nestedLoopsJoins.get_next();
            if (m == null) {
                bigt fs = new bigt("finalOutput");
                fs.deleteBigt();
                nestedLoopsJoins.close();
                break;
            }
            oBigt.mapInsert(m.getMapByteArray(), outbt, 1);
        }
        Stream km = oBigt.openStream();
        while (true) {
            Map m = km.getNext(new MID());
            if(m == null) break;
            m.print();
        }
    }

}