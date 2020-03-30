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

        boolean quit = false;
        ArrayList<CondExpr> select = new ArrayList<>();
        
        try {
            do {
                System.out.print(">> ");
                String que = sc.nextLine();
                String[] words = que.split("\\s+");
                if (words[0].equals("batchinsert")) {
                    String filepath = "/home/kaushal/DBMSI/Phase2/project2_testdata2.csv"; //words[1];
                    int type = 1;// Integer.parseInt(words[2]);
                    String dbname = "bd";//words[3];
                    SystemDefs sysdef = new SystemDefs(fpath + dbname, 8000, 500, "Clock");
                    try {
                        f = new bigt(dbname + "_" + String.valueOf(type), type);
                    } catch (Exception e) {
                        // status = FAIL;
                        System.err.println("*** Could not create heap file\n");
                        e.printStackTrace();
                    }
                    batchInsert(dbname, type, filepath);
                } else if (words[0].equals("query")) {
                    String dbname = words[1];
                    int type = Integer.parseInt(words[2]);
                    int order = Integer.parseInt(words[3]);
                    String filename = dbname+"_"+String.valueOf(type);
                    String rowFilter, colFilter, valFilter;
                    int bufpage;
                    if (words[4].charAt(0) == '[') {
                        // rowFilter = words[4] + words[5];
                        CondExpr c= new CondExpr();
                        c.fldNo = 1;
                        c.type1 = new AttrType(AttrType.attrSymbol);
                        c.op = new AttrOperator(AttrOperator.aopGT);
                        c.type2 = new AttrType(AttrType.attrString);
                        c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        c.operand2.string = words[4].substring(1, words[4].length()-1);
                        select.add(c);
                        c= new CondExpr();
                        c.fldNo = 1;
                        c.type1 = new AttrType(AttrType.attrSymbol);
                        c.op = new AttrOperator(AttrOperator.aopLT);
                        c.type2 = new AttrType(AttrType.attrString);
                        c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                        c.operand2.string = words[5].substring(0, words[5].length()-1);
                        select.add(c);
                        if (words[6].charAt(0) == '[') {

                            // colFilter = words[6] + words[7];
                            c= new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopGT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[6].substring(1, words[6].length()-1);
                            select.add(c);
                            c= new CondExpr();
                            c.fldNo = 2;
                            c.type1 = new AttrType(AttrType.attrSymbol);
                            c.op = new AttrOperator(AttrOperator.aopLT);
                            c.type2 = new AttrType(AttrType.attrString);
                            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                            c.operand2.string = words[7].substring(0, words[7].length()-1);
                            select.add(c);
                            if (words[8].charAt(0) == '[') {
                                // valFilter = words[8] + words[9];
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopGT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[8].substring(1, words[8].length()-1);
                                select.add(c);
                                c= new CondExpr();
                                c.fldNo = 4;
                                c.type1 = new AttrType(AttrType.attrSymbol);
                                c.op = new AttrOperator(AttrOperator.aopLT);
                                c.type2 = new AttrType(AttrType.attrString);
                                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                                c.operand2.string = words[9].substring(0, words[9].length()-1);
                                select.add(c);
                                bufpage = Integer.parseInt(words[10]);
                            } else {
                                valFilter = words[8];
                                if(!valFilter.equals("*")){
                                    c= new CondExpr();
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
                            if(!colFilter.equals("*")){
                                c= new CondExpr();
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
                        }
                    } else {
                        rowFilter = words[4];
                        if(!rowFilter.equals("*")){
                            CondExpr c;
                            c= new CondExpr();
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
                            c= new CondExpr();
                            c.fldNo = 2;
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
                            bufpage = Integer.parseInt(words[9]);
                            if (words[7].charAt(0) == '[') {

                                // valFilter = words[7] + words[8];
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
                                CondExpr c;
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
                                // valFilter = words[6] + words[7];
                                CondExpr c;
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
                                    CondExpr c;
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
                    }
                    // SystemDefs sysdef = new SystemDefs(fpath + dbname, 8000, bufpage, "Clock");
                    f = new bigt(dbname + String.valueOf(type), type);
                    CondExpr[] newSel = new CondExpr[select.size()+1];
                    for(int i = 0;i< select.size();i++){
                        newSel[i] = select.get(i);
                    }
                    select.clear();
                    newSel[newSel.length - 1] = null;
                    query(filename, type, order, newSel);
                } else if (words[0].equals("exit")) {
                    quit = true;
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

    public static boolean batchInsert(String dbFileName, int type, String filepath) throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException,
    InvalidSelectionException, IOException, UnknownKeyTypeException, GetFileEntryException,
    ConstructPageException, AddFileEntryException, IteratorException, HashEntryNotFoundException,
    InvalidFrameNumberException, PageUnpinnedException, ReplacerException{
        try {
            FileInputStream fin;
            short[] FldOffset = new short[5];
            fin = new FileInputStream(filepath);
            DataInputStream din = new DataInputStream(fin);
            BufferedReader bin = new BufferedReader(new InputStreamReader(din));
            
            BTreeFile btf = null;
            BTreeFile btf2 = null;
            BTreeFile file2, file3;

            f = new bigt(dbFileName+"_"+String.valueOf(type),type);

            btf = new BTreeFile("Adithya", 0, 100, 0);
            btf2 = new BTreeFile("AAAa", 1, 100, 0);

            // String line = bin.readLine();
            String line;
            int maplength = 0;
            int count = 0;
            StringTokenizer st;
            System.out.println("Batch Inserting records! Wait for few minutes!");
            while ((line = bin.readLine()) != null) {
                // System.out.println(line);
                st = new StringTokenizer(line);

                while (st.hasMoreTokens()) {
                    String token = st.nextToken();

                    StringTokenizer sv = new StringTokenizer(token);
                    String rowLabel = sv.nextToken(",");
                    maplength += (rowLabel.getBytes().length + 2);

                    String columnLabel = sv.nextToken(",");
                    maplength += (columnLabel.getBytes().length + 2);

                    int timeStamp = Integer.parseInt(sv.nextToken(","));
                    maplength += 4;

                    String value = sv.nextToken(",");
                    maplength += (value.getBytes().length + 2);

                    byte[] mapData = new byte[maplength + 10];

                    int position = 10;
                    ConvertMap.setStrValue(rowLabel, position, mapData);
                    position += rowLabel.getBytes().length + 2;

                    ConvertMap.setStrValue(columnLabel, position, mapData);
                    position += columnLabel.getBytes().length + 2;

                    ConvertMap.setIntValue(timeStamp, position, mapData);
                    position += 4;

                    ConvertMap.setStrValue(value, position, mapData);
                    position += value.getBytes().length + 2;

                    Map map = new Map(mapData, 0);

                    map.setHdr(new short[] { (short) rowLabel.getBytes().length, (short) columnLabel.getBytes().length,
                            (short) value.getBytes().length });

                    MID k = f.insertMap(map.getMapByteArray());


                    // System.out.println("Record No: " + count + ", MID: slt: " + k.slotNo + ",
                    // page:" + k.pageNo.pid);
                    
                    // if (type == 1) {
                    //     System.out.println("No type");
                    // }
                    if (type == 2) {
                        // System.out.println("starting point");
                        String key = map.getRowLabel();
                        // System.out.println(key);
                        btf.insert(new StringKey(key), k);

                    }
                    if (type == 3) {
                        String key = map.getColumnLabel();
                        btf.insert(new StringKey(key), k);
                    }
                    if (type == 4) {

                        String key1 = map.getRowLabel();
                        String key2 = map.getColumnLabel();
                        String key = key1 + key2;
                        int keyt = map.getTimeStamp();
                        // System.out.println(keyt);
                        btf.insert(new StringKey(key), k);
                        btf2.insert(new IntegerKey(keyt), k);
                    }
                    if (type == 5) {
                        String key1 = map.getRowLabel();
                        String key2 = map.getColumnLabel();
                        String key = key1 + key2;
                        int keyt = map.getTimeStamp();
                        btf.insert(new StringKey(key), k);
                        btf2.insert(new IntegerKey(keyt), k);
                    }


                }
                count++;
                System.out.println(count);
            }
            System.out.println("Hello "+ f.getMapCnt());

            file2 = new BTreeFile("Adithya", 0, 100, 0);
            BT.printBTree(btf.getHeaderPage());
            BT.printAllLeafPages(btf.getHeaderPage());
            // BT.printBTree(btf.new_scan(lo_key, hi_key));

            file3 = new BTreeFile("AAAa", 1, 100, 0);
            BT.printBTree(btf2.getHeaderPage());
            BT.printAllLeafPages(btf2.getHeaderPage());
            bin.close();

            System.out.println("Read counts: "+PCounter.rcounter);
            System.out.println("Write counts: "+PCounter.wcounter);

            System.out.println("Batchinsert finished!");

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        

        //Map m = indexScan.get_next();

        return true;
    }

    public static boolean query(String filename, int type, int order, CondExpr[] select)
            throws LowMemException, Exception {
        // Stream s = f.openStream(order, rowFilter, colFilter, valFilter);
        PCounter.initialize();
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
        
        System.out.println(Arrays.toString(select));
        // Sort s = null;

        FileScan fileScan = new FileScan(filename, 1, new short[]{32,32,32}, 4, proj_list, select);
        // if (order != 0) {
        //     s = new Sort(attrType,(short) 4, new short[]{32,32,32}, fileScan, order, new MapOrder(MapOrder.Ascending), 32, 300);
        // }
        
        Map map = new Map();
        MID mid = new MID();
        boolean done = false;
        int c = 0;
        System.out.println();
        while(!done){
            // if(order == 0){
                map = fileScan.get_next();
            // }
            // else{
            //     map = s.get_next();
            // }
            if(map == null){
                done = true;
            }else{
                map.print();
                c++;
            }
        }
        // System.out.println(c);
        System.out.println("ReadCount: "+PCounter.rcounter);
        System.out.println("WriteCount: "+PCounter.wcounter);
        System.out.println();
        return true;
    }
}