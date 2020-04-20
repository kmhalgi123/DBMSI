public boolean batchInsertOld(String filepath, int type, String dbfile){
    try{
        FileInputStream fin;
        short[] FldOffset = new short[5];
        // takes the filepath
        fin = new FileInputStream(filepath);
        // input from file
        DataInputStream din = new DataInputStream(fin);
        BufferedReader bin = new BufferedReader(new InputStreamReader(din));
        
        // initialize btreefile
        BTreeFile btf = null;

        // open btree file with filename batchinsert file, used for row_column index
        btf = new BTreeFile("batchinsert_file", 0, 100, 0);

        String line;

        int count = 0;
        StringTokenizer st;
        System.out.println("Batch Inserting records! Wait for few minutes!");
        while ((line = bin.readLine()) != null) {
            
            
            st = new StringTokenizer(line);

            while (st.hasMoreTokens()) {
                String token = st.nextToken();

                StringTokenizer sv = new StringTokenizer(token);
                String rowLabel = sv.nextToken(",");

                String columnLabel = sv.nextToken(",");

                int timeStamp = Integer.parseInt(sv.nextToken(","));

                String value = sv.nextToken(",");

                byte[] mapData = new byte[116];

                ConvertMap.setStrValue(rowLabel, 10, mapData);
                ConvertMap.setStrValue(columnLabel, 44, mapData);
                ConvertMap.setIntValue(timeStamp, 78, mapData);
                ConvertMap.setStrValue(value, 82, mapData);

                Map map = new Map(mapData, 0);

                map.setHdr(new short[] { 32,32,32 }); 

                MID k = insertMap(map.getMapByteArray());
                
                String key1 = map.getRowLabel();
                String key2 = map.getColumnLabel();
                String key = key2 + key1;
                btf.insert(new StringKey(key), k);
                
            }
            count++;
            System.out.println(count);
        }
        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        FldSpec[] proj_list = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        proj_list[0]= new FldSpec(rel, 1);
        proj_list[1]= new FldSpec(rel, 2);
        proj_list[2]= new FldSpec(rel, 3);
        proj_list[3]= new FldSpec(rel, 4);
        ArrayList<String> keyDone = new ArrayList<>();
        PriorityQueue<MapMID> pq = new PriorityQueue<MapMID>(5, new MapComparator());
        FileScan fs = new FileScan(dbfile, type, new short[]{32,32,32}, 4, proj_list, null);
        boolean done = false;
        while (!done) {
            Map map = fs.get_next();
            if(map == null){
                break;
            }
            map.mapSetup();
            // map.print();
            if(keyDone.contains(map.getColumnLabel() + map.getRowLabel())){
                continue;
            }else{
                keyDone.add(map.getColumnLabel() + map.getRowLabel());
            }
            CondExpr[] ex = new CondExpr[2];
            ex[0] = new CondExpr();
            ex[0].fldNo = 1;
            ex[0].type1 = new AttrType(AttrType.attrSymbol);
            ex[0].op = new AttrOperator(AttrOperator.aopEQ);
            ex[0].type2 = new AttrType(AttrType.attrString);
            ex[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            ex[0].operand2.string = map.getColumnLabel() + map.getRowLabel();
            IndexScan iScan = new IndexScan(new IndexType(IndexType.Column_Row_Label_Index), dbfile, "batchinsert_file", attrType, new short[]{32,32,32}, 4, 4, proj_list, null, 2, false, ex);
            boolean done2 = false;
            int c2 = 0;
            while(!done2){
                MapMID map2 = iScan.get_next_MapMid();
                if(map2 == null){
                    break;
                }
                map2.getMap().print();
                c2++;
                pq.add(map2);
            }
            // System.out.println("current queue size "+pq.size());
            int c3 = 0;
            while (!pq.isEmpty()){
                MapMID mm  = pq.poll();
                if(c3 > 2){
                    System.out.println(mm.getMap().getRowLabel());
                    deleteMap(mm.getMID());
                }
                c3++;
            }
            
            // System.out.println("current queue size after deletion"+pq.size());
        }
        System.out.println("Map Counts: "+getMapCnt());
        System.out.println("Read counts: "+PCounter.rcounter);
        System.out.println("Write counts: "+PCounter.wcounter);
        bin.close();
        // System.out.println("Batchinsert finished! Now Waiting for items to be deleted");

    } catch (Exception e) {
        // TODO: handle exception
        e.printStackTrace();
    }
    return true;
}




if (map.getColumnLabel().equals(columnName)) {
    if (hashMap.containsKey(map.getRowLabel())) {
        hashMap.put(map.getRowLabel(), new ArrayList<Integer>(Arrays.asList(map.getTimeStamp())));
            // bt1.insertMap(map.getMapByteArray());
        }
        else {
            hashMap.put(map.getRowLabel(), new ArrayList<Integer>());
        }
    } 
}





// second table 
Stream newStream = bt1.openStream();
done = false;
MID mid1 = new MID();
while (!done) {
    Map map12 = newStream.getNext(mid1);
    if (map12 == null)
        done = true;
    else {
        map12.mapSetup();
        map12.print();

    }
}
Stream rightStream = f2.openStream();
done = false;
System.out.println("break here");
MID mid2 = new MID();
java.util.Map<String, Integer> hashMap2 = new HashMap<>();
while (!done) {
    Map mapR = rightStream.getNext(mid2);
    if (mapR == null)
        done = true;
    else {
        if (mapR.getColumnLabel().equals(columnName)) {
            int t = mapR.getTimeStamp();
            if (hashMap2.containsKey(mapR.getRowLabel())) {

                int t2 = hashMap2.get(mapR.getRowLabel());
                System.out.println(t2);
                if (t2 < t) {
                    hashMap2.put(mapR.getRowLabel(), mapR.getTimeStamp());
                    bt2.insertMap(mapR.getMapByteArray());
                }
            } else {

                hashMap2.put(mapR.getRowLabel(), mapR.getTimeStamp());

            }
        }
    }
}

if (hashMap2.isEmpty()) {

    System.out.println("map is empty");
} else {

    System.out.println(hashMap2);
}

Stream stream1 = bt2.openStream();
done = false;
MID mid3 = new MID();
while (!done) {
    Map map12 = stream1.getNext(mid3);
    if (map12 == null)
        done = true;
    else {
        map12.mapSetup();
        map12.print();

    }
}


// First table
boolean done = false;
MID mid = new MID();
java.util.Map<String, Integer> hashMap = new HashMap<>();
Map map = new Map();
do {
    map = leftStream.getNext(mid);
    if (map == null)
        done = true;
    else {
        if (map.getColumnLabel().equals(columnName)) {
            int t = map.getTimeStamp(); // 19, 60
            System.out.println(t + "t");
            if (hashMap.containsKey(map.getRowLabel())) { //R1
                System.out.println(map.getRowLabel() + "row label");

                int t2 = hashMap.get(map.getRowLabel());
                 //  t2 = 19, t = 60
                 System.out.println(t2 + "t2");
                if (t2 < t) {
                    t2 = t;
                    hashMap.put(map.getRowLabel(), t2); // R1:60
                    bt1.insertMap(map.getMapByteArray());
                }
            }
            else {
            hashMap.put(map.getRowLabel(), t); // R1: 19
            }
        }
    }

} while (!done);

if (hashMap.isEmpty()) {

    System.out.println("map is empty");
} else {
    System.out.println(hashMap);
}







System.out.println("break here");

FldSpec[] proj_list2 = new FldSpec[4];
proj_list2[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
proj_list2[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
proj_list2[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
proj_list2[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);


NestedLoopsJoins inr = null;
try {
    inr = new NestedLoopsJoins(map1, 4, sizes1, map2, 4, sizes2, 100, stream1, newBigtableName, outFilter, null,
            proj_list2, 4);
} catch (Exception e) {
    e.printStackTrace();
}
Map mapRight = new Map();
try {
    while ((mapRight = inr.get_next()) != null) {
        mapRight.mapSetup();
        mapRight.print();

    }
} catch (Exception e) {
    e.printStackTrace();
}

try {
    inr.close();
} catch (Exception e) {
    e.printStackTrace();
}





Stream streamNew = bt3.openStream();
boolean done = false;
MID mid3 = new MID();
while (!done) {
    Map map12 = streamNew.getNext(mid3);
    if (map12 == null)
        done = true;
    else {
        map12.mapSetup();
        map12.print();

    }
}

Stream streamNew1 = bt4.openStream();
done = false;
MID mid4 = new MID();
while (!done) {
    Map map12 = streamNew1.getNext(mid4);
    if (map12 == null)
        done = true;
    else {
        map12.mapSetup();
        map12.print();

    }
}




				// case RelSpec.newCase:
				// switch (type2[perm_mat[i].offset - 1].attrType) {
				// 	case AttrType.attrInteger:
				// 		Jmap.setTimeStamp(m2.getTimeStamp());
				// 		break;
				// 	case AttrType.attrString:
				// 		if(i == 3) {
				// 			if(m1.getValue().equals(m2.getValue())) {
				// 			Jmap.setStrFld(i + 1, m2.getStrFld(perm_mat[i].offset));
				// 		}
				// 		else 
				// 	}
