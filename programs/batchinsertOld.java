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
