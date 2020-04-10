package BigT;

import java.io.IOException;
import java.util.Comparator;

public class MapComparator implements Comparator<MapMID>{
    public int compare(MapMID m1, MapMID m2) {
        Map map1 = m1.getMap();
        Map map2 = m2.getMap();
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
 