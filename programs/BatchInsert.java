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
public class BatchInsert {
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
        f2.batchInsert("join2.csv", 1, "bigdata");

        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        Query3_CondExpr(outFilter, columnName);

        String table2 = "table2";
        bigt bt1 = new bigt("table1");
        bigt bt2 = new bigt(table2);

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
        m2.setTimeStamp(20);
        m1.mapSetup();
        m2.mapSetup();
        bt1.insertMap(m1.getMapByteArray());
        bt1.insertMap(m2.getMapByteArray());
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
        bt2.insertMap(m3.getMapByteArray());
        bt2.insertMap(m4.getMapByteArray());

        Stream stream1 = bt1.openStream();


        // Inner Join
        AttrType map1[] = new AttrType[4];
        map1[0] = new AttrType(AttrType.attrString);
        map1[1] = new AttrType(AttrType.attrString);
        map1[2] = new AttrType(AttrType.attrInteger);
        map1[3] = new AttrType(AttrType.attrString);

        // For all the Strings
        short sizes1[] = new short[3]; // [s1,s2,s3];
        sizes1[0] = 32;
        sizes1[1] = 32;
        sizes1[2] = 32;

        FldSpec[] map1Projection = new FldSpec[4];
        map1Projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        map1Projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        map1Projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        map1Projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        // Outer Join
        AttrType[] map2 = new AttrType[4];
        map2[0] = new AttrType(AttrType.attrString);
        map2[1] = new AttrType(AttrType.attrString);
        map2[2] = new AttrType(AttrType.attrInteger);
        map2[3] = new AttrType(AttrType.attrString);

        short[] sizes2 = new short[3];
        sizes2[0] = 32;
        sizes2[1] = 32;
        sizes2[2] = 32;

        FldSpec[] map2Projection = new FldSpec[4];
        map2Projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        map2Projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        map2Projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        map2Projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

        // Join table
        FldSpec[] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);

        FldSpec[] proj_listT3 = new FldSpec[4];
        proj_listT3[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_listT3[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_listT3[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        proj_listT3[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);


        
        // rowjoin join1 join2 outjoin C1 10

        // bt1 and bt2 will contain highest timestamps of tabl1 and 2

        // join bt1 and bt2
        // Stream stream1 = bt2.openStream();

        // rowjoin join1 join2 outjoin C1 100

        // First nested loop with table 1 and table 2
        String table4 = "table4";
        bigt bt3 = new bigt("table3");
        bigt bt4 = new bigt(table4);

        // creating table 3
        CondExpr[] selectT3 = new CondExpr[2];
        selectT3[0] = new CondExpr();
        selectT3[1] = new CondExpr();
        QueryT3_CondExpr(selectT3);
        NestedLoopsJoins nljT3 = null;
        try {
            nljT3 = new NestedLoopsJoins(map1, 4, sizes1, map2, 4, sizes2, 100, stream1, table2, selectT3, null,
                    proj_listT3, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map mapLeft = new Map();
        try {
            while ((mapLeft = nljT3.get_next()) != null) {
                mapLeft.mapSetup();
                // mapLeft.print();
                bt3.mapInsert(mapLeft.getMapByteArray());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            nljT3.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // printing bt3
        System.out.println("printing bt3");
        Stream streamT3 = bt3.openStream();
        boolean done = false;
        MID mid3 = new MID();
        while (!done) {
            Map mapT3 = streamT3.getNext(mid3);
            if (mapT3 == null)
                done = true;
            else {
                mapT3.mapSetup();
                mapT3.print();

            }
        }

        // creating table4 from topleft and topright
        System.out.println("break here");
        Stream streamNew1 = bt1.openStream();
        FldSpec[] proj_listT4 = new FldSpec[4];
        proj_listT4[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_listT4[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        proj_listT4[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        proj_listT4[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);

        NestedLoopsJoins inljT4 = null;
        try {
            inljT4 = new NestedLoopsJoins(map1, 4, sizes1, map2, 4, sizes2, 100, streamNew1, table2, outFilter, null,
                    proj_listT4, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map mapRight = new Map();
        try {
            while ((mapRight = inljT4.get_next()) != null) {
                mapRight.mapSetup();
                // mapRight.print();
                bt4.mapInsert(mapRight.getMapByteArray());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            inljT4.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // printing bt4
        System.out.println("bt4");
        Stream streamT4 = bt4.openStream();
        done = false;
        MID mid4 = new MID();
        while (!done) {
            Map mapT4p = streamT4.getNext(mid3);
            if (mapT4p == null)
                done = true;
            else {
                mapT4p.mapSetup();
                mapT4p.print();

            }
        }

        // rowjoin join1 join2 outjoin C1 100

        bigt bt5 = new bigt("table5");

        FldSpec[] proj_listT5 = new FldSpec[4];
        proj_listT5[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_listT5[1] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_listT5[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        proj_listT5[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        // R1,R3, R1:R3
        Stream streamT3New = bt3.openStream();
        NestedLoopsJoins nljT5 = null;
        try {
            nljT5 = new NestedLoopsJoins(map1, 4, sizes1, map2, 4, sizes2, 100, streamT3New, table4, outFilter, null,
                    proj_listT5, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map mapT5 = new Map();
        try {
            while ((mapT5 = nljT5.get_next()) != null) {
                mapT5.mapSetup();
                // mapRight.print();
                bt5.mapInsert(mapT5.getMapByteArray());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            nljT5.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // printing bt5
        System.out.println("printing bt5");
        Stream streamT5 = bt5.openStream();
        done = false;
        MID mid5 = new MID();
        while (!done) {
            Map mapT5p = streamT5.getNext(mid5);
            if (mapT5p == null)
                done = true;
            else {
                mapT5p.mapSetup();
                mapT5p.print();

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

    public static void updateNumbuf(int numbf) throws HashOperationException, PageUnpinnedException,
            PagePinnedException, PageNotFoundException, BufMgrException, IOException {
        SystemDefs.JavabaseBM.forcedFlush();
        new SystemDefs(fpath + "bigdata", 0, numbf + 50, "Clock");
    }

    public static void counterReInit() {
        System.out.println("Read Counter for this operation: " + PCounter.rcounter);
        System.out.println("Write Counter for this operation: " + PCounter.wcounter);
        PCounter.initialize();
    }
}