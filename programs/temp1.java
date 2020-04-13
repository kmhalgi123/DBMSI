    //     // iterate through every map in left stream
    //     while (!done) {
    //         Map m = leftStream.getNext(mid1);

    //         //MID m2 =  mm.getMID();
    //         if (m == null) {
    //             done = true;
    //         } else {
    //             // if the column name of map from left stream matches given column name, then consider only top 3 values
    //             // so while adding we poll the highes value and look at the count, if count is > 3, just remove the highest value
    //             if (m.getColumnLabel().equalsIgnoreCase(columnName)){
    //                 m.mapSetup();
    //                 mm.setMID(mid1);
    //                 mm.setMap(m);
    //             m.print();
                
    //             //outbt.insertMap(m.getMapByteArray());
    //             pq.add(mm);
    //             c++;
    //             }
    //         }
    //     }
        
    //     System.out.println("break here");
    //     done = false;
    //     while (!done) {
    //         Map m = rightStream.getNext(mid2);
    //         if (m == null) {
    //             done = true;
    //         } else {
    //             if (m.getColumnLabel().equalsIgnoreCase(columnName)){

    //             m.mapSetup();
    //             mm2.setMID(mid2);
    //             mm2.setMap(m);
    //             m.print();
    //             //outbt.insertMap(m.getMapByteArray());
    //             pq2.add(mm2);
    //             c++;
    //             }
    //         }
    //     }
    //     // using nested while loop for R1:R2 format
    //     System.out.println("polling highest value");
    //     // get highest timestamp for each row, check if they are equal
    //     mm = pq.poll();
    //     mm.getMap().print();

    //     System.out.println("polling highest value for join2");
    //     // get highest timestamp for each row, check if they are equal
    //     mm2  = pq2.poll();
    //     mm2.getMap().print();
    //     Map combined = null;
    //    if(mm.getMap().getValue().equals(mm2.getMap().getValue()))
    //    {
    //        combined = new Map();
    //        combined.setRowLabel(mm.getMap().getRowLabel()+mm2.getMap().getRowLabel());
    //        combined.setColumnLabel(mm.getMap().getColumnLabel());
    //        if(mm.getMap().getTimeStamp() > mm2.getMap().getTimeStamp())           
    //             combined.setTimeStamp(mm.getMap().getTimeStamp());
    //         else 
    //             combined.setTimeStamp(mm2.getMap().getTimeStamp());
    //        combined.setValue(mm.getMap().getValue());
    //        outbt.insertMap(combined.getMapByteArray());
           
    //    }
  


      //pq.poll().getMap().print();
        //pq.poll().getMap().print();
        //pq.poll().getMap().print();
        //pq.poll().getMap().print();
        // if they are equal, get the top 3 values.

        // not printing outbt rightnow
        // done = false;
        // System.out.println("printing the joined map");
        // Stream outstream = outbt.openStream();
        // while (!done) {
            
        //     Map m = outstream.getNext(mid);
        //     if (m == null) {
        //         done = true;
        //     } else {
        //         m.mapSetup();
        //         m.print();
        //         c++;
                
        //     }

            // use inner while loop

            
        // }
