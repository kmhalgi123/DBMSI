package programs;

import BigT.*;
import global.*;
import iterator.*;

/**
 * RowSort does sort the table on the most recent value of given colname
 * Implemeted using a priorityQueue over a sort.java
 */

public class RowSort extends TestDriver {

    @Override
    public void performCmd(String[] words) {
        // TODO Auto-generated method stub
        String inbtname = words[1];
        String outbtname = words[2];
        String colname = words[3];
        int order = Integer.parseInt(words[4]);
        int numbf = Integer.parseInt(words[5]);
        try {
            rowSort(inbtname, outbtname, colname, order, numbf);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void rowSort(String inbtname, String outbtname, String colname, int order, int numbf)
    throws UnknowAttrType, LowMemException, JoinsException, Exception {
        updateNumbuf(numbf);
        short[] s_sizes = {32,32,32};
        bigt outBigt = new bigt(outbtname);
        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        FldSpec[] proj_list = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        proj_list[0]= new FldSpec(rel, 1);
        proj_list[1]= new FldSpec(rel, 2);
        proj_list[2]= new FldSpec(rel, 3);
        proj_list[3]= new FldSpec(rel, 4);
        FileScan fs = new FileScan(inbtname, 1, s_sizes, 4, proj_list, null);
        Sort s = new Sort(fs, 1, new MapOrder(order), 32, numbf, 1, colname, inbtname);
        while(true){
            Map m = s.get_next();
            if(m == null) break;
            outBigt.insertMap(m.getMapByteArray());
            m.print();
        }
        s.close();
    }
    
}