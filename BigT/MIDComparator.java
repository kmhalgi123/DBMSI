package BigT;

import java.util.Comparator;
import BigT.*;
import global.*;

public class MIDComparator implements Comparator<MID>{

    @Override
    public int compare(MID o1, MID o2) {
        try{
            if (o1.pageNo.pid > o2.pageNo.pid) 
                return 1; 
            else if (o1.pageNo.pid < o2.pageNo.pid) 
                return -1; 
            else{
                if (o1.slotNo > o2.slotNo)
                    return 1;
                else if (o1.slotNo > o2.slotNo)
                    return -1;
                else 
                    return 0;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }
    
}
