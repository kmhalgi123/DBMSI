while (!done1){
    Map left = leftStream.getNext(mid1);
    if (left == null) {
        done1 = true;
    } else {
        while(!done2){

            Map right = rightStream.getNext(mid2);
            if(right == null) {
                done2 = true;
            }
            else 
            {
                String leftColumnLabel = left.getColumnLabel();
                String rightColumnLabel = right.getColumnLabel();
                if (leftColumnLabel.equalsIgnoreCase(columnName) && rightColumnLabel.equalsIgnoreCase(columnName) && leftColumnLabel.equalsIgnoreCase(rightColumnLabel))
                  {  //select top 3 values
                    left.mapSetup();
                    right.mapSetup();
                    mm.setMID(mid1);
                    mm.setMap(left);
                    pq.add(mm);

                    mm2.setMID(mid2);
                    mm2.setMap(right);
                    pq2.add(mm2);

                }
                else {
                    Map newmap = new Map();
                    
                    outbt.insertMap();                    
                }

            }
         
        }
   
    }
}