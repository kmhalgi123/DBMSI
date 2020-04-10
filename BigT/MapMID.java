package BigT;

import java.io.IOException;

import global.MID;

public class MapMID {
    private MID mid;
    private Map map;

    public MapMID() {
    }

    public MapMID(MID mid, Map map) {
        this.map = map;
        this.mid = mid;
    }

    public Map getMap() {
        return map;
    }

    public MID getMID() {
        return mid;
    }

    public void setMID(MID mid) {
        this.mid = mid;
    }

    public void setMap(Map map) throws IOException {
        map.mapSetup();
        this.map = map;
        
    }
}