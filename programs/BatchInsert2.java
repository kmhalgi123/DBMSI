package programs;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.Iterator;
import java.util.List;
import BigT.bigt;
// import global.*;
import java.util.StringTokenizer;
import java.util.TreeSet;

import BigT.*;
import btree.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import iterator.*;
import index.*;

// batchinsert project2_testdata.csv 2 bigtable2
// batchinsert small1.csv 2 bigtable2
// batchinsert small2.csv 2 bigtable2
public class BatchInsert2 {
    static String fpath = "";
    static bigt f = null;
    static bigt f1 = null;
    static bigt f2 = null;
    static bigt outbt = null;
    static String outbtname;
    static String bigTFileName;
    String dbFileName = "project2_testdata.csv";

    public static void main(String[] args) {

        PCounter.initialize();
        Scanner sc = new Scanner(System.in);
        // SystemDefs sysdef = new SystemDefs(fpath + "database", 500, 500, "Clock");

        // SystemDefs sysdef = new SystemDefs(fpath + "bigdata", 8000, 500, "Clock");

        new SystemDefs(fpath + "bigdata", 20000, 500, "Clock");
        boolean quit = false;
        ArrayList<CondExpr> select = new ArrayList<>();
        Map map = new Map();
        try {
            do {
                System.out.print(">> ");

                String que = sc.nextLine();
                String[] words = que.split("\\s+");

                if (words[0].equals("batchinsert")) {
                    String filepath = words[1];
                    int type = Integer.parseInt(words[2]);
                    String dbname = words[3];
                    // if (f == null) {
                    // bigTFileName = dbname + "_" + String.valueOf(type);

                    // try {
                    // f = new bigt(bigTFileName);
                    // batchInsert(bigTFileName, type, filepath);

                    // } catch (Exception e) {
                    // System.err.println("Could not create heap file \n");
                    // e.printStackTrace();
                    // }

                    // }
                    // else {
                    // f = new bigt(bigTFileName);
                    // System.out.println("Here");
                    // System.out.println(f.getMapCnt());
                    // batchInsert(bigTFileName, type, filepath);

                    // }
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
                        c.op = new AttrOperator(AttrOperator.aopEQ);
                        c.type2 = new AttrType(AttrType.attrString);
                        c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        c.operand2.string = words[4].substring(1, words[4].length() - 1);
                        select.add(c);
                        c = new CondExpr();
                        c.fldNo = 1;
                        c.type1 = new AttrType(AttrType.attrSymbol);
                        c.op = new AttrOperator(AttrOperator.aopEQ);
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
                    query(bigTFileName, type, order, newSel);
                } else if (words[0].equals("exit")) {
                    quit = true;
                } else if (words[0].equals("mapinsert")) {
                    int type = Integer.parseInt(words[5]);
                    String dbname = words[6];
                    String rl = words[1];
                    String cl = words[2];
                    String val = words[3];
                    int ts = Integer.parseInt(words[4]);
                    int numbf = Integer.parseInt(words[7]);
                    f = new bigt(dbname);
                    byte[] mapData = new byte[116];

                    int position = 10;
                    ConvertMap.setStrValue(rl, position, mapData);
                    position += 34; // rowLabel.getBytes().length + 2;

                    ConvertMap.setStrValue(cl, position, mapData);
                    position += 34; // columnLabel.getBytes().length + 2;

                    ConvertMap.setStrValue(rl, position, mapData);
                    position += 34;

                    ConvertMap.setStrValue(val, position, mapData);
                    position += 34; // value.getBytes().length + 2;

                    ConvertMap.setIntValue(ts, position, mapData);
                    position += 4;

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

                    f1.batchInsert("join1.csv", 1, "bigdata");

                    Stream leftStream = f1.openStream();
                    RowJoin(amt_of_mem, leftStream, btname2, columnName);

                } else {
                    System.out.println("Invalid input!");
                }
            } while (!quit);
        } catch (Exception e) {
            // TODO: handle exception
            // System.out.println(e.printStackTrace());
            e.printStackTrace();
        }

    }

    private static void Query3_CondExpr(CondExpr[] expr, String columnName) {

        expr[0].next = null;
        expr[0].fldNo = 2;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

        expr[0].operand2.string = columnName;
        expr[1] = null;
    }

    // Row join function
    // rowjoin join1 join2 outjoin C1 10

    public static boolean RowJoin(int amt_of_mem, Stream leftStream, String rightBigtName, String columnName)
            throws Exception {

        // opening the right bigt and outbt
        f2 = new bigt(rightBigtName);
        outbt = new bigt(outbtname);
        // insert data into rightbt and open the stream
        f2.batchInsert("join2.csv", 1, rightBigtName);

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        Query3_CondExpr(outFilter, columnName);

        String tableTopRight = "tableTopRight";
        bigt btTopLeft = new bigt("topleft");
        bigt btTopRight = new bigt(tableTopRight);

        // manual insertion code
        // [R1, C1, 12, 650] - into top timestamps 1
        // [R2, C1, 12, 300] - into top timestamps 1
        Map m1 = new Map();
        m1.setHdr(new short[] { 32, 32, 32 });
        m1.setRowLabel("R1");
        m1.setColumnLabel("C1");
        m1.setValue("650");
        m1.setTimeStamp(12);
        Map m2 = new Map();
        m2.setHdr(new short[] { 32, 32, 32 });
        m2.setRowLabel("R2");
        m2.setColumnLabel("C1");
        m2.setValue("300");
        m2.setTimeStamp(12);
        m1.mapSetup();
        m2.mapSetup();
        btTopLeft.insertMap(m1.getMapByteArray());
        btTopLeft.insertMap(m2.getMapByteArray());
        // manual insertion
        // [R3, C1, 16, 650] - into top time timestamp2
        // [R4, C1, 30, 300] - into top time timestamp2

        Map m3 = new Map();
        m3.setHdr(new short[] { 32, 32, 32 });
        m3.setRowLabel("R3");
        m3.setColumnLabel("C1");
        m3.setTimeStamp(16);
        m3.setValue("650");

        Map m4 = new Map();
        m4.setHdr(new short[] { 32, 32, 32 });
        m4.setRowLabel("R4");
        m4.setColumnLabel("C1");
        m4.setTimeStamp(30);
        m4.setValue("300");

        m3.mapSetup();

        // m3.print();
        m4.mapSetup();
        btTopRight.insertMap(m3.getMapByteArray());
        btTopRight.insertMap(m4.getMapByteArray());


        System.out.println("Printing table Top left");
        Boolean topleftdone = false;
        Stream streamt3 = btTopLeft.openStream();
        MID tlmid = new MID();
        while (!topleftdone) {
            Map mT3 = streamt3.getNext(tlmid);
            if (mT3 == null) {
                topleftdone = true;
            } else {
                mT3.mapSetup();
                mT3.print();
            }
        }


        System.out.println("Printing table Top right");
         topleftdone = false;
        Stream streamt4 = btTopRight.openStream();
        MID tlmid1 = new MID();
        while (!topleftdone) {
            Map mT3 = streamt4.getNext(tlmid1);
            if (mT3 == null) {
                topleftdone = true;
            } else {
                mT3.mapSetup();
                mT3.print();
            }
        }

        Stream stream1 = btTopLeft.openStream();


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
        MID topRightMid = new MID();
        MID topLeftMid = new MID();
        // int counter = 0;
        int outcounter = 0;
        Map combined_map;
        short sizes[] = new short[3];
        int c3 = 0;
        
        //assume that we have topleft and top right at this point

        // join top left, top right based on value 
        
        Stream topLeftStream = btTopLeft.openStream();
        Stream topRightStream = btTopRight.openStream();

        String t3 = "T3";
        bigt T3 = new bigt(t3);
        String t4 = "T4";
        bigt T4 = new bigt(t4);
        Boolean topLeftDone = false;
        Boolean topRightDone = false;
        System.out.println("we are just checking the values");
        outer: while(!topLeftDone){
            Map topLeftmap = topLeftStream.getNext(topLeftMid);
            System.out.println("printing top left map");
            // topLeftmap.print();
            if (topLeftmap == null) {
                topLeftDone = true;
                //System.out.println("breaking outer");
                break outer;
            }
            
                topLeftmap.print();
            topRightDone = false;
            topRightStream = btTopRight.openStream();
            inner: while(!topRightDone){
                
                Map topRightMap = topRightStream.getNext(topRightMid);
                    if(topRightMap == null){
                        topRightDone = true;
                        break inner;
                    }
                    // we assume that we have top left and top right at this point
                    
                    System.out.println(topLeftmap.getValue() + " top left map");
                    System.out.println(topRightMap.getValue() + " top right map");
                    if(topLeftmap.getValue().equalsIgnoreCase(topRightMap.getValue())){
                        // add left map to T3 
                        T3.mapInsert(topLeftmap.getMapByteArray());
                        // add right map to T4
                        T4.mapInsert(topRightMap.getMapByteArray());
                    }

            }
        }
        

        System.out.println("Printing table T3");
        done = false;
        Stream streamT3 = T3.openStream();
        while (!done) {
            Map mT3 = streamT3.getNext(mid);
            if (mT3 == null) {
                done = true;
            } else {
                mT3.mapSetup();
                mT3.print();
                c++;
            }
        }

        System.out.println("Printing table T4");
        done = false;
        Stream streamT4 = T4.openStream();
        while (!done) {
            Map mT4 = streamT4.getNext(mid);
            if (mT4 == null) {
                done = true;
            } else {
                mT4.mapSetup();
                mT4.print();
                c++;
            }
        }

        // create T5 by joining T3 and T4 based on value
        // three fields 
        // first field  R1, second field = R2, third field = R1:R2
    // String 

        MID t3Mid = new MID();
        MID t4Mid = new MID();
        Stream t3Stream = T3.openStream();
        Stream t4Stream = T4.openStream();
        Boolean t3Done = false;
        Boolean t4Done = false;
        String t5 = "T5";
        bigt T5 = new bigt(t5);
        outer2: while(!t3Done){
                Map t3Map = t3Stream.getNext(t3Mid);
                if (t3Map == null){
                    t3Done = true;
                    break outer2;
                }
            t4Done = false;
            t4Stream = T4.openStream();
            inner2: while(!t4Done){
                
                Map t4Map =  t4Stream.getNext(t4Mid);
                if (t4Map == null){
                    t4Done = true;
                    break inner2;
                }
                if (t3Map.getValue().equalsIgnoreCase(t4Map.getValue())){
                    Map tempMap = new Map();
                    
                    //combined_map.setHdr(sizes);
                    tempMap.setHdr(new short[] { 32, 32, 32 });
                    tempMap.setRowLabel(t3Map.getRowLabel());
                    tempMap.setColumnLabel(t4Map.getRowLabel());
                    tempMap.setTimeStamp(1);
                    tempMap.setValue(t3Map.getRowLabel()+":"+t4Map.getRowLabel());
                    // our map will be of the form [R1, R2, 1, R1:R2]
                    tempMap.mapSetup();
                    T5.mapInsert(tempMap.getMapByteArray());
                    
                    
                }
            }
        }
        
        System.out.println("Printing table T5");
        done = false;
        Stream streamT5 = T5.openStream();
        while (!done) {
            Map mT5 = streamT5.getNext(mid);
            if (mT5 == null) {
                done = true;
            } else {
                mT5.mapSetup();
                mT5.print();
                c++;
            }
        }
        

        // leftStream.closescan();
        // Map newLeftStream = leftStream.openStream();
        //left table join with T5 fld 1, // we have left stream
        Boolean t5Done = false;
        Boolean leftDone = false;
        Stream t5Stream;
        MID t5Mid = new MID();
        MID leftMid = new MID();
        bigt finalOutput = new bigt("finalOutput");
        outer3: while(!leftDone){
            Map leftMap = leftStream.getNext(leftMid);
            if(leftMap == null){
                leftDone = true;
                break outer3;
            }
            t5Done = false;
            t5Stream = T5.openStream();
            inner3: while(!t5Done){
                Map t5Map = t5Stream.getNext(t5Mid);
                if(t5Map == null){
                    t5Done = true;
                    break inner3;
                }

                if(t5Map.getRowLabel().equalsIgnoreCase(leftMap.getRowLabel())){
                    if(leftMap.getColumnLabel().equalsIgnoreCase(columnName)){
                        Map tempMap2 = new Map();
                        tempMap2.setHdr(new short[] { 32, 32, 32 });
                        tempMap2.setRowLabel(t5Map.getValue());
                        tempMap2.setColumnLabel(leftMap.getColumnLabel());
                        tempMap2.setTimeStamp(leftMap.getTimeStamp());
                        tempMap2.setValue(leftMap.getValue());
                        // our map will be of the form [R1, R2, 1, R1:R2]
                        tempMap2.mapSetup();
                        finalOutput.mapInsert(tempMap2.getMapByteArray());
                    }
                    else{
                        Map tempMap2 = new Map();
                        tempMap2.setHdr(new short[] { 32, 32, 32 });
                        tempMap2.setRowLabel(t5Map.getValue());
                        tempMap2.setColumnLabel(leftMap.getColumnLabel()+"_left");
                        tempMap2.setTimeStamp(leftMap.getTimeStamp());
                        tempMap2.setValue(leftMap.getValue());
                        // our map will be of the form [R1, R2, 1, R1:R2]
                        tempMap2.mapSetup();
                        finalOutput.mapInsert(tempMap2.getMapByteArray());
                    }
                }
            }
        }


        //right table join with T5 fld 2, // we have right bigtname
        t5Done = false;
        Boolean rightDone = false;
        t5Stream = T5.openStream();
        t5Mid = new MID();
        MID rightMid = new MID();

        bigt rightBigt = new bigt(rightBigtName);
        Stream rightStream = rightBigt.openStream();
        
        outer4: while(!t5Done){
            Map t5Map = t5Stream.getNext(t5Mid);
            if(t5Map == null){
                t5Done = true;
                break outer4;
            }
            leftDone = false;
            inner4: while(!rightDone){
                Map rightMap = rightStream.getNext(rightMid);
                if(rightMap == null){
                    rightDone = true;
                    break inner4;
                }

                if(t5Map.getColumnLabel().equalsIgnoreCase(rightMap.getRowLabel())){
                    if(rightMap.getColumnLabel().equalsIgnoreCase(columnName)){
                        Map tempMap3 = new Map();
                        tempMap3.setHdr(new short[] { 32, 32, 32 });
                        tempMap3.setRowLabel(t5Map.getValue());
                        tempMap3.setColumnLabel(rightMap.getColumnLabel());
                        tempMap3.setTimeStamp(rightMap.getTimeStamp());
                        tempMap3.setValue(rightMap.getValue());
                        // our map will be of the form [R1, R2, 1, R1:R2]
                        tempMap3.mapSetup();
                        finalOutput.mapInsert(tempMap3.getMapByteArray());
                    }
                    else{
                        Map tempMap3 = new Map();
                        tempMap3.setHdr(new short[] { 32, 32, 32 });
                        tempMap3.setRowLabel(t5Map.getValue());
                        tempMap3.setColumnLabel(rightMap.getColumnLabel()+"_right");
                        tempMap3.setTimeStamp(rightMap.getTimeStamp());
                        tempMap3.setValue(rightMap.getValue());
                        // our map will be of the form [R1, R2, 1, R1:R2]
                        tempMap3.mapSetup();
                        finalOutput.mapInsert(tempMap3.getMapByteArray());
                    }
                }
            }
        }

        System.out.println("final output");
        done = false;
        Stream stream = finalOutput.openStream();
        while (!done) {
            Map m = stream.getNext(mid);
            if (m == null) {
                done = true;
            } else {
                m.mapSetup();
                m.print();
                c++;
            }
        }


        return true;
    }

    private static void QueryT3_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    
        expr[0].fldNo = 4;
        expr[1] = null;
      }

  
  
  
    public static boolean query(String filename, int type, int order, CondExpr[] select)
            throws LowMemException, Exception {
        // Stream s = f.openStream(order, rowFilter, colFilter, valFilter);

        PCounter.initialize();
        int c = 0;
        CondExpr[] indexSelect = new CondExpr[2];
        indexSelect[0] = null;
        indexSelect[1] = null;

        short[] s_sizes = { 32, 32, 32 };
        f = new bigt(filename);
        // System.out.println("Query Map Count" + f.getMapCnt());
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
        short[] attrSize = { 32, 32, 32 };
        BTreeFile btf;
        boolean done = false;
        Sort s = null;

        if (type == 1) {

            // System.out.println(Arrays.toString(select));

            FileScan fileScan = new FileScan(filename, 1, new short[] { 32, 32, 32 }, 4, proj_list, select);
            if (order != 0) {
                s = new Sort(s_sizes, fileScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
            }

            Map map = new Map();
            MID mid = new MID();

            // System.out.println();
            while (!done) {
                if (order == 0) {
                    map = fileScan.get_next();
                } else {
                    map = s.get_next();
                }
                if (map == null) {
                    done = true;
                } else {
                    map.print();
                    // System.out.println(map.getMapByteArray().length);
                    c++;
                }
            }
            try {
                fileScan.close();
                if (s != null) {
                    s.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (type == 2) {
            btf = new BTreeFile("Adithya");
            if (select != null) {
                int i = 0;
                for (CondExpr condExpr : select) {
                    if (condExpr != null) {
                        if (condExpr.fldNo == 1) {
                            if (condExpr.op.attrOperator == AttrOperator.aopEQ) {
                                indexSelect[0] = condExpr;
                                indexSelect[1] = null;
                            } else {
                                indexSelect[i] = condExpr;
                                i++;
                            }
                        }
                    }
                }
            }
            try {
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Row_Label_Index), filename, "Adithya", attrType,
                        attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if (order != 0) {
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                // System.out.println();
                while (!done) {

                    if (order == 0) {
                        map = indexScan.get_next();
                    } else {
                        map = s.get_next();
                    }
                    if (map == null) {
                        done = true;
                    } else {
                        map.print();
                        c++;
                    }

                }

                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if (s != null) {
                        s.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (type == 3) {
            btf = new BTreeFile("Adithya");

            // select[0] = select[1];
            // select[1] = null;
            if (select != null) {
                int i = 0;
                for (CondExpr condExpr : select) {
                    if (condExpr != null) {
                        if (condExpr.fldNo == 2) {
                            if (condExpr.op.attrOperator == AttrOperator.aopEQ) {
                                indexSelect[0] = condExpr;
                                indexSelect[1] = null;
                            } else {
                                indexSelect[i] = condExpr;
                                i++;
                            }
                        }
                    }
                }
            }

            try {
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Column_Label_Index), filename, "Adithya", attrType,
                        attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if (order != 0) {
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                // System.out.println();
                while (!done) {

                    if (order == 0) {
                        map = indexScan.get_next();
                    } else {
                        map = s.get_next();
                    }
                    if (map == null) {
                        done = true;
                    } else {
                        map.print();
                        c++;
                    }

                }

                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if (s != null) {
                        s.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //
        if (type == 4) {
            btf = new BTreeFile("Adithya");
            // select = new CondExpr[2];
            // select[0].operand2.string += select[1].operand2.string;

            if (select != null) {
                int i = 0;
                // for(CondExpr condExpr : select){
                // if (condExpr != null){
                // if(condExpr.fldNo == 2){
                // if(condExpr.op.attrOperator == AttrOperator.aopEQ){
                // indexSelect[0] = condExpr;
                // indexSelect[1] = null;
                // }else{
                // indexSelect[i] = condExpr;
                // i++;
                // }
                // }
                // }
                // }
            }

            try {
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Column_Row_Label_Index), filename, "Adithya",
                        attrType, attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if (order != 0) {
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                // System.out.println();
                while (!done) {

                    if (order == 0) {
                        map = indexScan.get_next();
                    } else {
                        map = s.get_next();
                    }
                    if (map == null) {
                        done = true;
                    } else {
                        map.print();
                        c++;
                    }

                }

                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if (s != null) {
                        s.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (type == 5) {
            btf = new BTreeFile("Adithya");

            try {
                IndexScan indexScan = null;
                indexScan = new IndexScan(new IndexType(IndexType.Row_Label_Value_Index), filename, "Adithya", attrType,
                        attrSize, 4, 4, proj_list, select, 2, false, indexSelect);
                if (order != 0) {
                    s = new Sort(s_sizes, indexScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
                }
                Map map = new Map();
                // System.out.println();
                while (!done) {

                    if (order == 0) {
                        map = indexScan.get_next();
                    } else {
                        map = s.get_next();
                    }
                    if (map == null) {
                        done = true;
                    } else {
                        map.print();
                        c++;
                    }

                }

                btf.close();
                btf.destroyFile();
                try {
                    indexScan.close();
                    if (s != null) {
                        s.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("ReadCount: " + PCounter.rcounter);
        System.out.println("WriteCount: " + PCounter.wcounter);
        System.out.println();

        return true;
    }

    // public static void updateNumbuf(int numbf) throws HashOperationException, PageUnpinnedException,
    //         PagePinnedException, PageNotFoundException, BufMgrException, IOException {
    //     SystemDefs.JavabaseBM.forcedFlush();
    //     new SystemDefs(fpath + "bigdata", 0, numbf + 50, "Clock");
    // }

    public static void counterReInit() {
        System.out.println("Read Counter for this operation: " + PCounter.rcounter);
        System.out.println("Write Counter for this operation: " + PCounter.wcounter);
        PCounter.initialize();
    }
}