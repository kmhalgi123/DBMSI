package programs;

import global.PageId;
import global.SystemDefs;

public class DMTest{
    static boolean FAIL = true;
    public static void main(String[] args) {
        String f = "/home/kaushal/DBMSI/Phase2/";
        SystemDefs sysdef = new SystemDefs( f+args[0], 8193,  100, "Clock" );
        boolean status = FAIL;
        String name = "file" + 1; 
        PageId pgid = new PageId();
        pgid.pid = 0;
        try {
            SystemDefs.JavabaseDB.add_file_entry(name, pgid);
        }
        catch (java.io.IOException e){
            System.err.println("IOerror: " + e);
            status = FAIL;
            System.err.println ("*** Could not allocate a page");
            e.printStackTrace();
        }
        
        catch ( Exception e ) {
            status = FAIL;
            System.err.println ("*** Could not allocate a page");
            e.printStackTrace();
        }

        try {
            SystemDefs.JavabaseDB.add_file_entry(name,pgid);
        } catch (Exception e) {
            System.err.println ("**** Adding a duplicate file entry");
            System.out.println ("  --> Failed as expected \n");
            status = FAIL;
        }
        // catch (Exception e) {e.printStackTrace();}

    }
}