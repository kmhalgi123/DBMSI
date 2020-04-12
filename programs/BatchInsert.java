package programs;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
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

public class BatchInsert {
    static String fpath = "/tmp/";
    static bigt f = null;
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
                    try {
                        f = new bigt(dbname);
                    } catch (Exception e) {
                        // status = FAIL;
                        System.err.println("*** Could not create heap file\n");
                        e.printStackTrace();
                    }
                    batchInsert(dbname, type, filepath);
                } else if (words[0].equals("query")) {
                    String dbname = words[1];
                    int order = Integer.parseInt(words[2]);
                    String rowFilter, colFilter, valFilter;
                    int bufpage;
                    if (words[3].charAt(0) == '[') {
                        // rowFilter = words[4] + words[5];
                        CondExpr c= new CondExpr();
                        c.fldNo = 1;
                        c.type1 = new AttrType(AttrType.attrSymbol);
                        c.op = new AttrOperator(AttrOperator.aopGT);
                        c.type2 = new AttrType(AttrType.attrString);
                        c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        c.operand2.string = words[3].substring(1, words[3].length()-1);
                        select.add(c);
                        c= new CondExpr();
                        c.fldNo = 1;
                        c.type1 = new AttrType(AttrType.attrSymbol);
                        c.op = new AttrOperator(AttrOperator.aopLT);
                        c.type2 = new AttrType(AttrType.attrString);
                        c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        c.operand2.string = words[4].substring(0, words[4].length()-1);
                        select.add(c);
                        if (words[5].charAt(0) == '[') {

                            // colFilter = words[6] + words[7];
                            c= new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopGT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[5].substring(1, words[5].length()-1);
                            select.add(c);
                            c= new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopLT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[6].substring(0, words[6].length()-1);
                            select.add(c);
                            if (words[7].charAt(0) == '[') {
                                // valFilter = words[8] + words[9];
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[7].substring(1, words[7].length()-1);
                                select.add(c);
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[8].substring(0, words[8].length()-1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[9]);
                            } else {
                                valFilter = words[7];
                                if(!valFilter.equals("*")){
                                    c= new CondExpr();
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
                            if(!colFilter.equals("*")){
                                c= new CondExpr();
                                c.fldNo = 2;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopEQ);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[5];
                                select.add(c);
                            }
                            if (words[6].charAt(0) == '[') {
                                // valFilter = words[7] + words[8];
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[6].substring(1, words[6].length()-1);
                                select.add(c);
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[7].substring(0, words[7].length()-1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[8]);
                            } else {
                                valFilter = words[6];
                                if(!valFilter.equals("*")){
                                    c= new CondExpr();
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
                    } else {
                        rowFilter = words[3];
                        if(!rowFilter.equals("*")){
                            CondExpr c;
                            c= new CondExpr();
                            c.fldNo = 1;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopEQ);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[3];
                            select.add(c);
                        }
                        if (words[4].charAt(0) == '[') {
                            CondExpr c;
                            c= new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopGT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[4].substring(1, words[4].length()-1);
                            select.add(c);
                            c= new CondExpr();
                            c.fldNo = 4;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopLT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[5].substring(0, words[5].length()-1);
                            select.add(c);
                            if (words[6].charAt(0) == '[') {

                                // valFilter = words[7] + words[8];
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[6].substring(1, words[6].length()-1);
                                select.add(c);
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[7].substring(0, words[7].length()-1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[8]);
                            } else {
                                valFilter = words[6];
                                if(!valFilter.equals("*")){
                                    c= new CondExpr();
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
                        } else {
                            colFilter = words[4];
                            if(!colFilter.equals("*")){
                                CondExpr c;
                                c= new CondExpr();
                                c.fldNo = 2;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopEQ);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[4];
                                select.add(c);
                            }
                            if (words[5].charAt(0) == '[') {
                                // valFilter = words[6] + words[7];
                                CondExpr c;
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[5].substring(1, words[5].length()-1);
                                select.add(c);
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[6].substring(0, words[6].length()-1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[7]);
                            } else {
                                valFilter = words[5];
                                if(!valFilter.equals("*")){
                                    CondExpr c;
                                    c= new CondExpr();
                                    c.fldNo = 4;
                                    c.type1 = new AttrType(AttrType.attrSymbol);
                                    c.op = new AttrOperator(AttrOperator.aopEQ);
                                    c.type2 = new AttrType(AttrType.attrString);
                                    c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                    c.operand2.string = words[5];
                                    select.add(c);
                                }
                                bufpage = Integer.parseInt(words[6]);
                            }
                        }
                    }
                    CondExpr[] newSel = new CondExpr[select.size()+1];
                    for(int i = 0;i< select.size();i++){
                        newSel[i] = select.get(i);
                    }
                    select.clear();
                    newSel[newSel.length - 1] = null;
                    query(dbname, order, newSel, bufpage);
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
                    position += 34;

                    ConvertMap.setStrValue(cl, position, mapData);
                    position += 34; 

                    ConvertMap.setIntValue(ts, position, mapData);
                    position += 4;

                    ConvertMap.setStrValue(val, position, mapData);
                    position += 34; 

                    Map map = new Map(mapData, 0);

                    map.setHdr(new short[] { 32,32,32}); 

                    f.mapInsert(map.getMapByteArray(), dbname, type);
                } else if (words[0].equals("getCount")) {
                    String dbname = words[1];
                    f = new bigt(dbname);
                    System.out.println("Map count in "+dbname+": "+f.getMapCnt()+ "\nDistinct row count: "+f.getRowCnt()+"\nDistinct column count: "+f.getColumnCnt());
                } else if (words[0].equals("rowsort")) {
                    String inbtname = words[1];
                    String outbtname = words[2];
                    String colname = words[3];
                    int numbf = Integer.parseInt(words[4]);
                    rowSort(inbtname, outbtname, colname, numbf);
                }else {
                    System.out.println("Invalid input!");
                }
            } while (!quit);
        } catch (Exception e) {
            // System.out.println(e.printStackTrace());
            e.printStackTrace();
        }
    }

    public static void rowSort(String inbtname, String outbtname, String colname, int numbf)
            throws UnknowAttrType, LowMemException, JoinsException, Exception {
        short[] s_sizes = {32,32,32};
        bigt outBigt = new bigt(outbtname);
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
        FileScan fs = new FileScan(inbtname, 1, s_sizes, 4, proj_list, null);
        Sort s = new Sort(fs, 1, new MapOrder(MapOrder.Ascending), 32, numbf, 1, colname, inbtname);
        while(true){
            Map m = s.get_next();
            if(m == null) break;
            outBigt.insertMap(m.getMapByteArray());
            m.print();
        }
        s.close();
    }

    public static boolean batchInsert(String dbFileName, int type, String filepath) throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException,
    InvalidSelectionException, IOException, UnknownKeyTypeException, GetFileEntryException,
    ConstructPageException, AddFileEntryException, IteratorException, HashEntryNotFoundException,
    InvalidFrameNumberException, PageUnpinnedException, ReplacerException, HFDiskMgrException,
    HFBufMgrException, HFException {
        BTreeFile btf2 = new BTreeFile(dbFileName+"_2", 0, 100, DeleteFashion.NAIVE_DELETE);
        BTreeFile btf3 = new BTreeFile(dbFileName+"_3", 0, 100, DeleteFashion.NAIVE_DELETE);
        BTreeFile btf4 = new BTreeFile(dbFileName+"_4", 0, 100, DeleteFashion.NAIVE_DELETE);
        BTreeFile btf5 = new BTreeFile(dbFileName+"_5", 0, 100, DeleteFashion.NAIVE_DELETE);
        BTreeFile btf_insert = new BTreeFile(dbFileName+"_insert", 0, 100, DeleteFashion.NAIVE_DELETE);
        f = new bigt(dbFileName);
        f.batchInsert(filepath, type, dbFileName);
        return true;
    }

    public static boolean query(String filename, int order, CondExpr[] select, int numbuf)
            throws LowMemException, Exception {
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
        
            

        FileScan fileScan = new FileScan(filename, 1, s_sizes, 4, proj_list, select);
        Sort s = new Sort(s_sizes, fileScan, order, new MapOrder(MapOrder.Ascending), 32, 300, order);
        
        
        Map map = new Map();
        
        System.out.println();
        while(true){
            map = s.get_next();
            if(map == null){
                break;
            }
            map.print();
            c++;
        }
        try {
            // fileScan.close();
            s.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
}