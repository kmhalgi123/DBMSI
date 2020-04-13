 
    public static boolean RowJoin(int amt_of_mem, Stream leftStream, String rightBigtName, String columnName) throws Exception {
        
        // opening the right bigt and outbt
        f2 = new bigt(rightBigtName);
        outbt = new bigt(outbtname);
        
        //insert data into rightbt and open the stream
        f2.batchInsert("join2.csv", 1, "bigdata");
        Stream rightStream;
        // start priority queue on both left and right bigt because we need to compare the latest values from each row in left and right streams
        Set<Map> set = new TreeSet<>();
        Set<Map> set2 = new TreeSet<>();
        // Set<Map> set3 = new TreeSet<>();

        PriorityQueue<MapMID> pq = new PriorityQueue<MapMID>(1, new MapComparator().reversed());
        PriorityQueue<MapMID> pq2 = new PriorityQueue<MapMID>(1, new MapComparator().reversed());
        MID mid = new MID();  
        MID mid1 = new MID();
        MID mid2 = new MID();
        Boolean done = false;
        Boolean done1 = false;
        Boolean done2 = false;
        int c = 0;
        MapMID mm = new MapMID();
        MapMID mm2 = new MapMID();

        Boolean leftdone = false;
        MID leftmid = new MID();
        Boolean rightdone = false;
        MID rightmid;

        // Outer while loop
        outer: while (!leftdone) {
            //System.out.println("into outer");
            Map leftmap = leftStream.getNext(leftmid);
            Map combined_map;
            if (leftmap == null) {
                leftdone = true;
                //System.out.println("breaking outer");
                break outer;
            }
           
            
            rightdone = false;  
            rightmid = new MID();
            rightStream = f2.openStream();

            // Inner while loop
            inner: while(!rightdone){
 
                Map rightmap = rightStream.getNext(rightmid);
                if(rightmap == null){
                    //System.out.println("hello");
                    rightdone = true;
                    break inner;
                }
 

                 if (leftmap.getColumnLabel().equals(columnName) && rightmap.getColumnLabel().equals(columnName) ){
                    leftmap.mapSetup();
                    rightmap.mapSetup();

                    // mm.setMID(leftmid);
                    // mm.setMap(leftmap);
                    // mm2.setMID(rightmid);
                    // mm2.setMap(rightmap);
                    // //pq.add(mm);
                    set.add(leftmap);
                    set2.add(rightmap);
                }
                    // if (leftmap.getColumnLabel().equalsIgnoreCase(columnName)){
             
                    if (leftmap.getColumnLabel().equalsIgnoreCase(rightmap.getColumnLabel())){
                    if (leftmap.getColumnLabel().equalsIgnoreCase(columnName)){
                        // add left map
                        
                        // mm.setMID(leftmid);
                        // mm.setMap(leftmap);
                        
                        // set.add(mm);
                        combined_map = new Map(); 

                        short sizes[] = new short[3]; //[s1,s2,s3];
                         
                        String a  = leftmap.getRowLabel().substring(1)+":"+rightmap.getRowLabel().substring(1);
                        String b = leftmap.getColumnLabel();
                        int e = leftmap.getTimeStamp();
                        String d = leftmap.getValue();
                        sizes[0] = (short) (leftmap.getRowLabel().length()+1+rightmap.getRowLabel().length());
                        sizes[1] = (short) (leftmap.getColumnLabel().length());
                        sizes[2] = (short) (leftmap.getValue().length());
                        
                        combined_map.setHdr(sizes);                        
                        
                        combined_map.setRowLabel(a);
                        combined_map.setColumnLabel(b);
                        combined_map.setTimeStamp(e);
                        combined_map.setValue(d);
                        combined_map.mapSetup();

                        // Print the maps
                        // combined_map.print();

                        outbt.insertMap(combined_map.getMapByteArray());
                        
                        

                        // only have top 3 values
                            // top 3 condition yet to be added
                    }
                    else{  //if onlt left and right map columns are equal, but not equal to given column name
                        // add left map
                        combined_map = new Map(); 

                             
                        String a  = leftmap.getRowLabel().substring(1)+":"+rightmap.getRowLabel().substring(1);
                        String b = leftmap.getColumnLabel();
                        int e = leftmap.getTimeStamp();
                        String d = leftmap.getValue();
                        short sizes[] = new short[3];

                        sizes[0] = (short) (leftmap.getRowLabel().length()+1+rightmap.getRowLabel().length());
                        sizes[1] = (short) (leftmap.getColumnLabel().length());
                        sizes[2] = (short) (leftmap.getValue().length());
                        
                        combined_map.setHdr(sizes);
                        
                        combined_map.setRowLabel(a);
                        combined_map.setColumnLabel(b);
                        combined_map.setTimeStamp(e);
                        combined_map.setValue(d);
                        combined_map.mapSetup();
                        

                        // combined_map.print();

                        outbt.insertMap(combined_map.getMapByteArray());
                        
                        // add right map
                        combined_map = new Map(); 
   
                        a = leftmap.getRowLabel().substring(1)+":"+rightmap.getRowLabel().substring(1);
                        b = rightmap.getColumnLabel();
                        e = rightmap.getTimeStamp();
                        d = rightmap.getValue();
                        
                        sizes[0] = (short) (leftmap.getRowLabel().length()+1+rightmap.getRowLabel().length());
                        sizes[1] = (short) (rightmap.getColumnLabel().length());
                        sizes[2] = (short) (rightmap.getValue().length());
                        
                        combined_map.setHdr(sizes);
                        
                        combined_map.setRowLabel(a);
                        combined_map.setColumnLabel(b);
                        combined_map.setTimeStamp(e);
                        combined_map.setValue(d);
                        combined_map.mapSetup();
                        

                        // combined_map.print();

                        outbt.insertMap(combined_map.getMapByteArray());
                     
                    }
                }
            }

        }
        // int c3 = 0;
                
        // while (!pq.isEmpty()){
        //     MapMID mm5  = pq.poll();
        //     if(c3 > 2){
        //         mm5.getMap().print();
        //     }
        //     c3++;
        // }
        // while (!pq.isEmpty()){
        //     pq.poll().getMap().print();
        // }

        // rowjoin join1 join2 outjoin column1 100

        System.out.println("break");

        for (Map map : set) {
            System.out.println(map);
        }
    //     Iterator<Map> it = set.iterator();
    //     while(it.hasNext()) {
    //         it.next().print();
    //  }
        System.out.println("break");
        // while (!pq2.isEmpty()){
        //     pq2.poll().getMap().print();
        // }

    //     Iterator<Map> it2 = set2.iterator();
    //     while(it2.hasNext()) {
    //         it2.next().print();
    //  }
        
        // pq.poll().getMap().print();
        // pq2.poll().getMap().print();

       System.out.println("combined output");
       done = false;
       Stream stream = outbt.openStream();
       while (!done) {
           Map m = stream.getNext(mid);
           if (m == null) {
               done = true;
           } else {
               if (m.getColumnLabel().equalsIgnoreCase(columnName)){
               m.mapSetup();
               //mm2.setMID(mid);
               //jmm2.setMap(m);


            //    m.print();
               //outbt.insertMap(m.getMapByteArray());
               //pq2.add(mm2);
               c++;
               }
           }
       }

      
        
     

        // iterate through f1 and add them to outbt
    // remove the maps where recent values of column dont match
    // remove the column which is repeated twice (the common column)

    return true;

    }
    

    if()