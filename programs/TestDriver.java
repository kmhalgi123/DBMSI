package programs;

import bufmgr.*;
import diskmgr.*;
import global.*;

public abstract class TestDriver {
    public abstract void performCmd(String[] words);

    public void updateNumbuf(int numbuf) {
        try {
            SystemDefs.JavabaseBM.forcedFlush();
            new SystemDefs(GlobalConst.DBFILEPATH, 0, numbuf, "Clock");
        } catch (PageNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (BufMgrException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (HashOperationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (PagePinnedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void counterReInit(){
        System.out.println("Read Counter for this operation: "+PCounter.rcounter);
        System.out.println("Write Counter for this operation: "+PCounter.wcounter);
        PCounter.initialize();
    }
}