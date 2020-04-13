package iterator;

import java.io.IOException;
import java.util.Comparator;

import BigT.*;

public class MapComparator2 implements Comparator<pnode> {
    public int compare(pnode m1, pnode m2) {
        Map map1 = m1.map;
        Map map2 = m2.map;
        try {
            map1.mapSetup();
            map2.mapSetup();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try{
            if (map1.getTimeStamp() < map2.getTimeStamp()) 
                return 1; 
            else if (map1.getTimeStamp() > map2.getTimeStamp()) 
                return -1; 
            return 0; 
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }
}
