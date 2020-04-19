package programs;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
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
    // rowjoin join1 join2 outjoin column1 100

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


        CondExpr[] indexSelect = new CondExpr[2];
        indexSelect[0] = null;
        indexSelect[1] = null;

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

        bigt f = null;
        try {
          f = new bigt("BigTFile");
        } catch (Exception e) {
          e.printStackTrace();
        }
    


        Stream rightStream = null;

        try {
            rightStream = new Stream(f);
          } catch (Exception e) {
            e.printStackTrace();
          }
      

        Map map = new Map();
        try {
            map.setHdr(sizes1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        BTreeFile btf = null;
        try {
            btf = new BTreeFile("innerJoin", 0, 32, 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        MID mid = new MID();
        Map temp = null;
        try {
            temp = leftStream.getNext(mid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map newMap = new Map();
        String key = null;
        while (temp != null) {
            newMap.mapCopy(temp);

            try {
                key = newMap.getColumnLabel();
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                btf.insert(new StringKey(key), mid);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                temp = leftStream.getNext(mid);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        leftStream.closescan();


        IndexScan indexScan = null;
        // indexScan = new IndexScan(new IndexType(IndexType.Row_Label_Index), filename, "Adithya", attrType,
        //         attrSize, 4, 4, proj_list, select, 2, false, indexSelect);

        System.out.print("After building BTree Index on Column Label\n");
        try {
          indexScan = new IndexScan(new IndexType(IndexType.Column_Label_Index), "BigTFile", "innerJoin", map1, sizes1, 4, 4, map1Projection, outFilter, 2, false, indexSelect);
        }    
        catch (Exception e) {
            System.err.println("Error creating scan for Index scan");
          }

        NestedLoopsJoins inl = null;
        try {
            inl = new NestedLoopsJoins(map1, 4, sizes1, map2, 4, sizes2, 100, indexScan, rightBigtName, outFilter,
                    null, proj_list, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map mapLeft = new Map();
        try {
            while ((mapLeft = inl.get_next()) != null) {
                mapLeft.mapSetup();
                mapLeft.print();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            inl.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    // public static boolean batchInsert(String dbFileName, int type, String
    // filepath) throws IndexException, InvalidTypeException,
    // InvalidTupleSizeException, UnknownIndexTypeException,
    // InvalidSelectionException, IOException, UnknownKeyTypeException,
    // GetFileEntryException,
    // ConstructPageException, AddFileEntryException, IteratorException,
    // HashEntryNotFoundException,
    // InvalidFrameNumberException, PageUnpinnedException, ReplacerException,
    // HFDiskMgrException,
    // HFBufMgrException, HFException {
    // f = new bigt(dbFileName+"_"+String.valueOf(type));
    // f.batchInsert(filepath, type, dbFileName+"_"+String.valueOf(type));
    // return true;
    // }

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