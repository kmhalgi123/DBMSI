package programs;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Scanner;
// import global.*;
import java.util.StringTokenizer;

import BigT.*;
import btree.*;
import bufmgr.*;
import diskmgr.PCounter;
import global.*;
import iterator.*;
import index.*;

// batchinsert project2_testdata.csv 2 bigtable2
// batchinsert small1.csv 2 bigtable2
// batchinsert small2.csv 2 bigtable2
public class BatchInsert {
    static String fpath = "";
    static bigt f = null;
    static bigt f1 = null;
    static bigt f2 = null;
    static bigt sorted_f1 = null;
    static bigt sorted_f2 = null;
    static bigt outbt = null;
    static String outbtname;
    String dbFileName = "project2_testdata.csv";

    public static void main(String[] args) {

        PCounter.initialize();
        Scanner sc = new Scanner(System.in);

        SystemDefs sysdef = new SystemDefs(fpath + "bigdata", 8000, 500, "Clock");

        boolean quit = false;
        ArrayList<CondExpr> select = new ArrayList<>();

        try {
            do {
                System.out.print(">> ");
                String que = sc.nextLine();
                String[] words = que.split("\\s+");

                if (words[0].equals("batchinsert")) {
                    String filepath = words[1];
                    int type = Integer.parseInt(words[2]);
                    String dbname = words[3];
                    // SystemDefs sysdef = new SystemDefs(fpath + dbname, 8000, 500, "Clock");
                    f = new bigt(dbname + "_" + String.valueOf(type));
                    f.batchInsert(filepath, type, dbname + "_" + String.valueOf(type));

                } else if (words[0].equals("query")) {
                    String dbname = words[1];
                    int type = Integer.parseInt(words[2]);
                    int order = Integer.parseInt(words[3]);
                    String filename = dbname + "_" + String.valueOf(type);
                    String rowFilter, colFilter, valFilter;
                    int bufpage;
                    if (words[4].charAt(0) == '[') {
                        // rowFilter = words[4] + words[5];
                        CondExpr c = new CondExpr();
                        c.fldNo = 1;
                        c.type1 = new AttrType(AttrType.attrSymbol);
                        c.op = new AttrOperator(AttrOperator.aopGT);
                        c.type2 = new AttrType(AttrType.attrString);
                        c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        c.operand2.string = words[4].substring(1, words[4].length() - 1);
                        select.add(c);
                        c = new CondExpr();
                        c.fldNo = 1;
                        c.type1 = new AttrType(AttrType.attrSymbol);
                        c.op = new AttrOperator(AttrOperator.aopLT);
                        c.type2 = new AttrType(AttrType.attrString);
                        c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        c.operand2.string = words[5].substring(0, words[5].length() - 1);
                        select.add(c);
                        if (words[6].charAt(0) == '[') {

                            // colFilter = words[6] + words[7];
                            c = new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopGT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[6].substring(1, words[6].length() - 1);
                            select.add(c);
                            c = new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopLT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[7].substring(0, words[7].length() - 1);
                            select.add(c);
                            if (words[8].charAt(0) == '[') {
                                // valFilter = words[8] + words[9];
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[8].substring(1, words[8].length() - 1);
                                select.add(c);
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[9].substring(0, words[9].length() - 1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[10]);
                            } else {
                                valFilter = words[8];
                                if (!valFilter.equals("*")) {
                                    c = new CondExpr();
                                    c.fldNo = 4;
                                    c.type1 = new AttrType(AttrType.attrSymbol);
                                    c.op = new AttrOperator(AttrOperator.aopEQ);
                                    c.type2 = new AttrType(AttrType.attrString);
                                    c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                    c.operand2.string = words[9];
                                    select.add(c);
                                }
                                bufpage = Integer.parseInt(words[9]);
                            }
                        } else {
                            colFilter = words[6];
                            if (!colFilter.equals("*")) {
                                c = new CondExpr();
                                c.fldNo = 2;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopEQ);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[6];
                                select.add(c);
                            }
                            if (words[7].charAt(0) == '[') {
                                // valFilter = words[7] + words[8];
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[7].substring(1, words[7].length() - 1);
                                select.add(c);
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[8].substring(0, words[8].length() - 1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[9]);
                            } else {
                                valFilter = words[7];
                                if (!valFilter.equals("*")) {
                                    c = new CondExpr();
                                    c.fldNo = 4;
                                    c.type1 = new AttrType(AttrType.attrSymbol);
                                    c.op = new AttrOperator(AttrOperator.aopEQ);
                                    c.type2 = new AttrType(AttrType.attrString);
                                    c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                    c.operand2.string = words[7];
                                    select.add(c);
                                }
                                bufpage = Integer.parseInt(words[8]);
                            }
                        }
                    } else {
                        rowFilter = words[4];
                        if (!rowFilter.equals("*")) {
                            CondExpr c;
                            c = new CondExpr();
                            c.fldNo = 1;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopEQ);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[4];
                            select.add(c);
                        }
                        if (words[5].charAt(0) == '[') {
                            // colFilter = words[5] + words[6];
                            CondExpr c;
                            c = new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopGT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[5].substring(1, words[5].length() - 1);
                            select.add(c);
                            c = new CondExpr();
                            c.fldNo = 4;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopLT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[6].substring(0, words[6].length() - 1);
                            select.add(c);
                            bufpage = Integer.parseInt(words[9]);
                            if (words[7].charAt(0) == '[') {

                                // valFilter = words[7] + words[8];
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[7].substring(1, words[7].length() - 1);
                                select.add(c);
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[8].substring(0, words[8].length() - 1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[9]);
                                bufpage = Integer.parseInt(words[9]);
                            } else {
                                valFilter = words[7];
                                if (!valFilter.equals("*")) {
                                    c = new CondExpr();
                                    c.fldNo = 4;
                                    c.type1 = new AttrType(AttrType.attrSymbol);
                                    c.op = new AttrOperator(AttrOperator.aopEQ);
                                    c.type2 = new AttrType(AttrType.attrString);
                                    c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                    c.operand2.string = words[7];
                                    select.add(c);
                                }
                                bufpage = Integer.parseInt(words[8]);
                            }
                        } else {
                            colFilter = words[5];
                            if (!colFilter.equals("*")) {
                                CondExpr c;
                                c = new CondExpr();
                                c.fldNo = 2;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopEQ);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[5];
                                select.add(c);
                            }
                            if (words[6].charAt(0) == '[') {
                                // valFilter = words[6] + words[7];
                                CondExpr c;
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[6].substring(1, words[6].length() - 1);
                                select.add(c);
                                c = new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[7].substring(0, words[7].length() - 1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[8]);
                            } else {
                                valFilter = words[6];
                                if (!valFilter.equals("*")) {
                                    CondExpr c;
                                    c = new CondExpr();
                                    c.fldNo = 4;
                                    c.type1 = new AttrType(AttrType.attrSymbol);
                                    c.op = new AttrOperator(AttrOperator.aopEQ);
                                    c.type2 = new AttrType(AttrType.attrString);
                                    c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                    c.operand2.string = words[6];
                                    select.add(c);
                                }
                                bufpage = Integer.parseInt(words[7]);
                            }
                        }
                    }
                    // SystemDefs sysdef = new SystemDefs(fpath + dbname, 8000, bufpage, "Clock");
                    f = new bigt(dbname + String.valueOf(type));
                    CondExpr[] newSel = new CondExpr[select.size() + 1];
                    for (int i = 0; i < select.size(); i++) {
                        newSel[i] = select.get(i);
                    }
                    select.clear();
                    newSel[newSel.length - 1] = null;
                    query(filename, type, order, newSel);
                } else if (words[0].equals("exit")) {
                    quit = true;
                } else if (words[0].equals("mapinsert")) {
                    String dbname = words[5];
                    String rl = words[1];
                    String cl = words[2];
                    String val = words[3];
                    int ts = Integer.parseInt(words[4]);
                    int numbf = Integer.parseInt(words[6]);
                    f = new bigt(dbname);
                    byte[] mapData = new byte[116];

                    int position = 10;
                    ConvertMap.setStrValue(rl, position, mapData);
                    position += 34; // rowLabel.getBytes().length + 2;

                    ConvertMap.setStrValue(cl, position, mapData);
                    position += 34; // columnLabel.getBytes().length + 2;

                    ConvertMap.setIntValue(ts, position, mapData);
                    position += 4;

                    ConvertMap.setStrValue(val, position, mapData);
                    position += 34; // value.getBytes().length + 2;

                    Map map = new Map(mapData, 0);

                    map.setHdr(new short[] { 32, 32, 32 });

                    MID k = f.mapInsert(map.getMapByteArray());
                } else if (words[0].equals("rowjoin")) {
                    String btname1 = words[1];
                    String btname2 = words[2];
                    outbtname = words[3];
                    String columnName = words[4];
                    String numbuf = words[5];
                    int amt_of_mem = Integer.parseInt(numbuf);
                    f1 = new bigt(btname1);
                    f1.batchInsert("simple1.csv", 1, btname1);

                    // this will sort the given bigt and put those values into sorted_f1
                    rowSort(btname1, btname1+"sorted", columnName, amt_of_mem);
                    sorted_f1 = new bigt(btname1+"sorted");

                    System.out.println("sorted left ");
                    boolean done = false;
                    Stream stream = sorted_f1.openStream();

                    System.out.println("sorted values");
                    MID mid = new MID();  
                    while (!done) {
                        Map m = stream.getNext(mid);
                        if (m == null) {
                            done = true;
                        } else {
                            m.mapSetup();
                            m.print();
                        }
                    }

                    Stream leftStream = sorted_f1.openStream();



                    RowJoin(amt_of_mem, leftStream, btname2, columnName);

                }  
                else {
                    System.out.println("Invalid input!");
                }
            } while (!quit);
        } catch (Exception e) {
            // TODO: handle exception
            // System.out.println(e.printStackTrace());
            e.printStackTrace();
        }

    }

    public static void rowSort(String inbtname, String outbtname, String colname, int numbf)
            throws UnknowAttrType, LowMemException, JoinsException, Exception {
        short[] s_sizes = { 32, 32, 32 };
        bigt outBigt = new bigt(outbtname);
        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        FldSpec[] proj_list = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        proj_list[0] = new FldSpec(rel, 1);
        proj_list[1] = new FldSpec(rel, 2);
        proj_list[2] = new FldSpec(rel, 3);
        proj_list[3] = new FldSpec(rel, 4);
        FileScan fs = new FileScan(inbtname, 1, s_sizes, 4, proj_list, null);
        Sort s = new Sort(fs, 1, new MapOrder(MapOrder.Ascending), 32, numbf, 1, colname, inbtname);
        while (true) {
            Map m = s.get_next();
            if (m == null)
                break;
            outBigt.insertMap(m.getMapByteArray());
            m.print();
        }
        s.close();
    }


    // Row join function
    // rowjoin join1 join2 outjoin column1 100
    // rowjoin join1 join2 outjoin C1 100
    public static boolean RowJoin(int amt_of_mem, Stream leftStream, String rightBigtName, String columnName) throws Exception {
        
        // opening the right bigt and outbt
        f2 = new bigt(rightBigtName);
        outbt = new bigt(outbtname);
        
        //insert data into rightbt and open the stream
        //f2.batchInsert("join2.csv", 1, "bigdata");
        f2.batchInsert("simple2.csv", 1, rightBigtName);

        rowSort(rightBigtName, rightBigtName+"sorted", columnName, amt_of_mem);
        sorted_f2 = new bigt(rightBigtName+"sorted");
        Stream rightStream = sorted_f2.openStream();

        
        // start priority queue on both left and right bigt because we need to compare the latest values from each row in left and right streams
        // PriorityQueue<MapMID> pq = new PriorityQueue<MapMID>(5, new MapComparator().reversed());
        // PriorityQueue<MapMID> pq2 = new PriorityQueue<MapMID>(5, new MapComparator().reversed());
        MID mid = new MID();  
        // MID mid1 = new MID();
        // MID mid2 = new MID();
        Boolean done = false;
        // Boolean done1 = false;
        // Boolean done2 = false;
        int c = 0;
        // MapMID combined_mm = new MapMID();
        // MapMID mm2 = new MapMID();

        Boolean leftdone = false;
        MID leftmid = new MID();
        Boolean rightdone = false;
        MID rightmid;
        // int counter = 0;
        int outcounter = 0;
        Map combined_map;
        short sizes[] = new short[3];
        int c3 = 0;
        
        // half working
        outer: while (!leftdone) {
            //System.out.println("into outer");
            Map leftmap = leftStream.getNext(leftmid);
            if (leftmap == null) {
                leftdone = true;
                //System.out.println("breaking outer");
                break outer;
            }
            outcounter = 0;
            rightdone = false;
            rightmid = new MID();
            rightStream = sorted_f2.openStream();
            inner: while(!rightdone){
                
                Map rightmap = rightStream.getNext(rightmid);
                
                if(rightmap == null){
                    rightdone = true;
                    break inner;
                }
                     
                if (leftmap.getColumnLabel().equalsIgnoreCase(rightmap.getColumnLabel())){
                    if (leftmap.getColumnLabel().equalsIgnoreCase(columnName)){

                        // get the most recent value 
                        
                        combined_map = new Map(); 
                        // add left map
                       
                        sizes[0] = (short) (leftmap.getRowLabel().length()+1+rightmap.getRowLabel().length());
                        sizes[1] = (short) (leftmap.getColumnLabel().length()); 
                        sizes[2] = (short) (leftmap.getValue().length());
                        
                        //combined_map.setHdr(sizes);
                        combined_map.setHdr(new short[] { 32, 32, 32 });
                        
                        combined_map.setRowLabel(leftmap.getRowLabel()+":"+rightmap.getRowLabel());
                        combined_map.setColumnLabel(leftmap.getColumnLabel());
                        combined_map.setTimeStamp(leftmap.getTimeStamp());
                        combined_map.setValue(leftmap.getValue());
                        combined_map.mapSetup();
                        
                        combined_map.print();
                        outbt.mapInsert(combined_map.getMapByteArray());
                        //outbt.insertMap(combined_map.getMapByteArray());
                       
                        combined_map = new Map();  
                        // add right map
                       
                        sizes[0] = (short) (leftmap.getRowLabel().length()+1+rightmap.getRowLabel().length());
                        sizes[1] = (short) (rightmap.getColumnLabel().length());
                        sizes[2] = (short) (rightmap.getValue().length());
                        
                        //combined_map.setHdr(sizes);
                        combined_map.setHdr(new short[] { 32, 32, 32 });
                        
                        combined_map.setRowLabel(leftmap.getRowLabel()+":"+rightmap.getRowLabel());
                        combined_map.setColumnLabel(rightmap.getColumnLabel());
                        combined_map.setTimeStamp(rightmap.getTimeStamp());
                        combined_map.setValue(rightmap.getValue());
                        combined_map.mapSetup();
                        
                        combined_map.print();
                        
                        //MID combined_mid = outbt.insertMap(combined_map.getMapByteArray());
                        outbt.mapInsert(combined_map.getMapByteArray());
                         
                }
            

                    else{  //if onlt left and right map columns are equal, but not equal to given column name
                        
                        // get the most recent value 
                       
                        combined_map = new Map(); 
                        // add left map
                       
                        sizes[0] = (short) (leftmap.getRowLabel().length()+1+rightmap.getRowLabel().length());
                        sizes[1] = (short) (leftmap.getColumnLabel().length()); 
                        sizes[2] = (short) (leftmap.getValue().length());
                        
                        
                        //combined_map.setHdr(sizes);
                        combined_map.setHdr(new short[] { 32, 32, 32 });
                        
                        combined_map.setRowLabel(leftmap.getRowLabel()+":"+rightmap.getRowLabel());
                        combined_map.setColumnLabel("left_"+leftmap.getColumnLabel());
                        combined_map.setTimeStamp(leftmap.getTimeStamp());
                        combined_map.setValue(leftmap.getValue());
                        combined_map.mapSetup();
                        
                        combined_map.print();
                        outbt.mapInsert(combined_map.getMapByteArray());
                        //outbt.insertMap(combined_map.getMapByteArray());
                        
                        combined_map = new Map(); 
                        // add right map
                       
                        sizes[0] = (short) (leftmap.getRowLabel().length()+1+rightmap.getRowLabel().length());
                        sizes[1] = (short) (rightmap.getColumnLabel().length()); 
                        sizes[2] = (short) (rightmap.getValue().length());
                        
                        //combined_map.setHdr(sizes);
                        combined_map.setHdr(new short[] { 32, 32, 32 });
                        
                        combined_map.setRowLabel(leftmap.getRowLabel()+":"+rightmap.getRowLabel());
                        combined_map.setColumnLabel("right_"+rightmap.getColumnLabel());
                        combined_map.setTimeStamp(rightmap.getTimeStamp());
                        combined_map.setValue(rightmap.getValue());
                        combined_map.mapSetup();
                        
                        combined_map.print();
                        
                        //outbt.insertMap(combined_map.getMapByteArray());  
                        outbt.mapInsert(combined_map.getMapByteArray());        
                        
                    }
                }
            }

        }
       System.out.println("combined output");
       done = false;
       Stream stream = outbt.openStream();
       while (!done) {
           Map m = stream.getNext(mid);
           if (m == null) {
               done = true;
           } else {
               //if (m.getColumnLabel().equalsIgnoreCase(columnName)){
               m.mapSetup();
               //mm2.setMID(mid);
               //jmm2.setMap(m);
               m.print();
               //outbt.insertMap(m.getMapByteArray());
               //pq2.add(mm2);
               c++;
              // }
           }
       }
        // iterate through f1 and add them to outbt
    // remove the maps where recent values of column dont match
    // remove the column which is repeated twice (the common column)
    
    return true;
        }
    




    public static boolean query(String filename, int type, int order, CondExpr[] select)
            throws LowMemException, Exception {
        // Stream s = f.openStream(order, rowFilter, colFilter, valFilter);
        PCounter.initialize();
        int c = 0;
        CondExpr[] indexSelect = new CondExpr[2];
        indexSelect[0] = null;
        indexSelect[1] = null;

        short[] s_sizes = {32,32,32};

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
        short[] attrSize = {32,32,32};
        BTreeFile btf;
        boolean done = false;
        Sort s = null;

        if (type == 1){
            
            
            System.out.println(Arrays.toString(select));
            

            FileScan fileScan = new FileScan(filename, 1, new short[]{32,32,32}, 4, proj_list, select);
            if (order != 0) {
                s = new Sort(s_sizes, fileScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
            }
            
            Map map = new Map();
            MID mid = new MID();
            
            System.out.println();
            while(!done){
                if(order == 0){
                    map = fileScan.get_next();
                }
                else{
                    map = s.get_next();
                }
                if(map == null){
                    done = true;
                }else{
                    map.print();
                    // System.out.println(map.getMapByteArray().length);
                    c++;
                }
            }
            try {
                fileScan.close();
                if(s != null){
                    s.close();
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        
        if (type == 2) {
            btf = new BTreeFile("Adithya");
            if (select != null){
                int i = 0;
                for(CondExpr condExpr : select){
                    if (condExpr != null){
                        if(condExpr.fldNo == 1){
                            if(condExpr.op.attrOperator == AttrOperator.aopEQ){
                                indexSelect[0] = condExpr;
                                indexSelect[1] = null;
                            }else{
                                indexSelect[i] = condExpr;
                                i++;
                            }
                        }
                    }
                }
            }
            try{
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Row_Label_Index), filename, "Adithya", attrType, attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if(order != 0){
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                System.out.println();
                while(!done){
                    
                    if(order == 0){
                        map = indexScan.get_next();
                    }
                    else{
                        map = s.get_next();
                    }
                    if(map == null){
                        done = true;
                    }else{
                        map.print();
                        c++;
                    }
                    
                }
                
                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if(s != null){
                        s.close();
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        if (type == 3) {
            btf = new BTreeFile("Adithya");

            // select[0] = select[1];
            // select[1] = null;
            if (select != null){
                int i = 0;
                for(CondExpr condExpr : select){
                    if (condExpr != null){
                        if(condExpr.fldNo == 2){
                            if(condExpr.op.attrOperator == AttrOperator.aopEQ){
                                indexSelect[0] = condExpr;
                                indexSelect[1] = null;
                            }else{
                                indexSelect[i] = condExpr;
                                i++;
                            }
                        }
                    }
                }
            }

            try{
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Column_Label_Index), filename, "Adithya", attrType, attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if(order != 0){
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                System.out.println();
                while(!done){
                    
                    if(order == 0){
                        map = indexScan.get_next();
                    }
                    else{
                        map = s.get_next();
                    }
                    if(map == null){
                        done = true;
                    }else{
                        map.print();
                        c++;
                    }
                    
                }
                
                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if(s != null){
                        s.close();
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        //
        if (type == 4) {
            btf = new BTreeFile("Adithya");
            // select = new CondExpr[2];
            // select[0].operand2.string += select[1].operand2.string;

            if (select != null){
                int i = 0;
                // for(CondExpr condExpr : select){
                //     if (condExpr != null){
                //         if(condExpr.fldNo == 2){
                //             if(condExpr.op.attrOperator == AttrOperator.aopEQ){
                //                 indexSelect[0] = condExpr;
                //                 indexSelect[1] = null;
                //             }else{
                //                 indexSelect[i] = condExpr;
                //                 i++;
                //             }
                //         }
                //     }
                // }
            }

            try{
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Column_Row_Label_Index), filename, "Adithya", attrType, attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if(order != 0){
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                System.out.println();
                while(!done){
                    
                    if(order == 0){
                        map = indexScan.get_next();
                    }
                    else{
                        map = s.get_next();
                    }
                    if(map == null){
                        done = true;
                    }else{
                        map.print();
                        c++;
                    }
                    
                }
                
                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if(s != null){
                        s.close();
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        if (type == 5) {
            btf = new BTreeFile("Adithya");

            try {
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Row_Label_Value_Index), filename, "Adithya", attrType, attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if(order != 0){
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                System.out.println();
                while(!done){
                    
                    if(order == 0){
                        map = indexScan.get_next();
                    }
                    else{
                        map = s.get_next();
                    }
                    if(map == null){
                        done = true;
                    }else{
                        map.print();
                        c++;
                    }
                    
                }
                
                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if(s != null){
                        s.close();
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("ReadCount: " + PCounter.rcounter);
        System.out.println("WriteCount: " + PCounter.wcounter);
        System.out.println();

        return true;
    }
}