package BigT;

import global.MID;

public class MapMID {
    private MID mid;
    private Map map;

    public MapMID(){}

    public MapMID(MID mid, Map map){
        this.map = map;
        this.mid = mid;
    }

    public Map getMap(){
        return map;
    }

    public MID getMID(){
        return mid;
    }
}