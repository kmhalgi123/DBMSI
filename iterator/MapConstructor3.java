package iterator;

import global.*;

import java.util.Comparator;

public class MapConstructor3 implements Comparator<customPnode>{

    public int sortOrder;

    public MapConstructor3(int sortOrder){
        this.sortOrder = sortOrder;
    }

    @Override
    public int compare(customPnode m1, customPnode m2) {
        if(sortOrder == MapOrder.Ascending){
            int custom1 = m1.getTimestamp();
            int custom2 = m2.getTimestamp();
            try{
                if (custom1 > custom2) 
                    return 1; 
                else if (custom1 < custom2) 
                    return -1; 
                return 0; 
            }catch (Exception e){
                e.printStackTrace();
            }
            return 0;
        }else {
            int custom1 = m1.getTimestamp();
            int custom2 = m2.getTimestamp();
            try{
                if (custom1 < custom2) 
                    return 1; 
                else if (custom1 > custom2) 
                    return -1; 
                return 0; 
            }catch (Exception e){
                e.printStackTrace();
            }
            return 0;
        }
    }

}