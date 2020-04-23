package programs;

import BigT.*;
import global.*;

/**
 * Mapinsert: insert a single map in the bigt
 */
public class MapInsert extends TestDriver {

    @Override
    public void performCmd(String[] words) {
        int type = Integer.parseInt(words[5]);
        String dbname = words[6];
        String rl = words[1];
        String cl = words[2];
        String val = words[3];
        int ts = Integer.parseInt(words[4]);
        int numbf = Integer.parseInt(words[7]);

        updateNumbuf(numbf);
        try{
            bigt f = new bigt(dbname);
            byte[] mapData = new byte[116];

            int position = 10;
            ConvertMap.setStrValue(rl, position, mapData);
            position += 34;

            ConvertMap.setStrValue(cl, position, mapData);
            position += 34;

            ConvertMap.setIntValue(ts, position, mapData);
            position += 4;

            ConvertMap.setStrValue(val, position, mapData);
            position += 34;

            Map map = new Map(mapData, 0);

            map.setHdr(new short[] { 32, 32, 32 });

            f.mapInsert(map.getMapByteArray(), dbname, type);

            System.out.println("Map inserted!");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}