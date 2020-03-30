package programs;

import java.io.IOException;

import BigT.HFBufMgrException;
import BigT.HFDiskMgrException;
import BigT.HFException;
import BigT.InvalidTupleSizeException;
import BigT.Map;
import BigT.Stream;
import BigT.bigt;
import global.MID;
import global.SystemDefs;

/**
 * Query
 */
public class Query {

    public static void main(String[] args)
            throws HFDiskMgrException, HFBufMgrException, HFException, IOException, InvalidTupleSizeException {
        String fpath = "/home/kaushal/DBMSI/Phase2/";
        Stream stream = null;
        MID mid = new MID();
        String tableName = args[0];
        bigt f = null;
        int type = Integer.parseInt(args[1]);
        int order = Integer.parseInt(args[2]);
        String rowFilter = args[3];
        String columnFilter = args[4];
        String valueFilter = args[5];
        int numbf = Integer.parseInt(args[6]);

        if(rowFilter.equals("*")) rowFilter = "";
        if(columnFilter.equals("*")) columnFilter = "";
        if(valueFilter.equals("*")) valueFilter = "";

        SystemDefs s = new SystemDefs(fpath+tableName, 1000, numbf, "Clock");
        f = new bigt("file_1",type);
        // stream = f.openStream(order, rowFilter, columnFilter, valueFilter);
        Map map = new Map();
        boolean done = false;
        while(!done){
            // map = stream.getNext(mid);
            if(map == null){
                done = true;
            }else{
                map.print();
            }
        }

    }
}