package programs;

import BigT.Map;
import BigT.bigt;
import global.MID;
import global.SystemDefs;

/**
 * BigTTest
 */
public class BigTTest {

    public static void main(String[] args) {
        String m = "/home/kaushal/DBMSI/Phase2/";
        SystemDefs sysdef = new SystemDefs( m+args[0], 8193,  100, "Clock" );
        bigt f = null;
        try {
            f = new bigt("file_1");
        }
        catch (Exception e) {
            // status = FAIL;
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
        }
        try {
            MID mid1 = f.insertMap(new byte[]{0, 10, 0, 23, 0, 30, 0, 34, 0, 37, 0, 11, -17, -69, -65, 68, 111, 109, 105, 110, 105, 99, 97, 0, 5, 90, 101, 98, 114, 97, 0, 0, -77, -13, 0, 1, 49});
            MID mid2 = f.insertMap(new byte[]{0, 10, 0, 21, 0, 28, 0, 32, 0, 35, 0, 9, 83, 105, 110, 103, 97, 112, 111, 114, 101, 0, 5, 67, 97, 109, 101, 108, 0, 0, 36, 84, 0, 1, 50});
            Map am = f.getMap(mid2);
            am.mapSetup();
            am.print();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}