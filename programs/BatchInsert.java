package programs;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import BigT.*;
import btree.*;
import bufmgr.*;
import diskmgr.*;
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

        new SystemDefs(fpath + "bigdata", 20000, 500, "Clock");
        boolean quit = false;

        try {
            do {
                System.out.print(">> ");
                String que = sc.nextLine();
                String[] words = que.split("\\s+");
                if (words[0].equals("batchinsert")) {
                    batchInsertDriver(words);
                } else if (words[0].equals("query")) {
                    queryParser(words);
                } else if (words[0].equals("exit")) {
                    quit = true;
                } else if (words[0].equals("mapinsert")) {
                    mapInsertDriver(words);
                } else if (words[0].equals("getCounts")) {
                    getCountDriver(words);
                } else if (words[0].equals("rowsort")) {
                    rowSortDriver(words);
                } else if (words[0].equals("rowjoin")) {
                    rowJoinDriver(words);
                } else {
                    System.out.println("Invalid input!");
                }
                counterReInit();
            } while (!quit);
        } catch (Exception e) {
            // System.out.println(e.printStackTrace());
            e.printStackTrace();
        }

        sc.close();
    }

    public static void rowJoinDriver(String[] words) {
        String leftbt = words[1];
        String rightbt = words[2];
        String colfilter = words[3];
        String outbt = words[4];
        int numbf = Integer.parseInt(words[5]);
        try {
            rowJoin(leftbt, rightbt, colfilter, outbt, numbf);
        } catch (FileScanException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MapUtilsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidRelation e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JoinNewFailed e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JoinLowMemory e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SortException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JoinsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IndexException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (PageNotReadException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (PredEvalException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (LowMemException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnknowAttrType e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnknownKeyTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void rowJoin(String leftbt, String rightbt, String colFilter, String outbt, int numbf)
    throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException,
    PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException,
    Exception {
        updateNumbuf(numbf);
        FldSpec[] proj_list = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        bigt oBigt = new bigt(outbt);
        proj_list[0]= new FldSpec(rel, 1);
        proj_list[1]= new FldSpec(rel, 2);
        proj_list[2]= new FldSpec(rel, 3);
        proj_list[3]= new FldSpec(rel, 4);
        NestedLoopsJoins nestedLoopsJoins = new NestedLoopsJoins(colFilter, numbf, leftbt, rightbt, null, null, proj_list, 4);
        while (true) {
            Map m = nestedLoopsJoins.get_next();
            if (m == null) {
                bigt fs = new bigt("finalOutput");
                fs.deleteBigt();
                nestedLoopsJoins.close();
                break;
            }
            m.print();
            oBigt.mapInsert(m.getMapByteArray(), outbt, 1);
        }
    }

    public static void batchInsertDriver(String words[]) throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException,
    InvalidSelectionException, UnknownKeyTypeException, GetFileEntryException, ConstructPageException,
    AddFileEntryException, IteratorException, HashEntryNotFoundException, InvalidFrameNumberException,
    PageUnpinnedException, ReplacerException, HFDiskMgrException, HFBufMgrException, HFException,
    HashOperationException, PagePinnedException, PageNotFoundException, BufMgrException, IOException,
    InvalidSlotNumberException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
    IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
    DeleteRecException, IndexSearchException, LeafDeleteException, InsertException {
        String filepath = words[1];
        int type = Integer.parseInt(words[2]);
        String dbname = words[3];
        int numbf = Integer.parseInt(words[4]);
        try {
            f = new bigt(dbname);
        } catch (Exception e) {
            // status = FAIL;
            System.err.println("*** Could not create bigt file\n");
            e.printStackTrace();
        }
        batchInsert(dbname, type, filepath, numbf);
    }

    public static void queryParser(String[] words) throws LowMemException, Exception {
        ArrayList<CondExpr> select = new ArrayList<>();
        String max_c;
        int char_max;
        String dbname = words[1];
        int order = Integer.parseInt(words[2]);
        String rowFilter, colFilter, valFilter;
        int bufpage;
        if (words[3].charAt(0) == '[') {
            // rowFilter = words[4] + words[5];
            CondExpr c = new CondExpr();
            c.fldNo = 1;
            c.type1 = new AttrType(AttrType.attrSymbol);
            c.op = new AttrOperator(AttrOperator.aopGT);
            c.type2 = new AttrType(AttrType.attrString);
            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            c.operand2.string = words[3].substring(1, words[3].length() - 1);
            select.add(c);
            c = new CondExpr();
            c.fldNo = 1;
            c.type1 = new AttrType(AttrType.attrSymbol);
            c.op = new AttrOperator(AttrOperator.aopLT);
            c.type2 = new AttrType(AttrType.attrString);
            c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            max_c = words[4].substring(0, words[4].length() - 1);
            char_max = (int) max_c.charAt(max_c.length() - 1);
            char_max++;
            max_c = max_c.substring(0, max_c.length() - 1) + Character.toString((char) char_max);
            c.operand2.string = max_c;
            select.add(c);
            if (words[5].charAt(0) == '[') {

                // colFilter = words[6] + words[7];
                c = new CondExpr();
                c.fldNo = 2;
                c.type1 = new AttrType(AttrType.attrSymbol);
                c.op = new AttrOperator(AttrOperator.aopGT);
                c.type2 = new AttrType(AttrType.attrString);
                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                c.operand2.string = words[5].substring(1, words[5].length() - 1);
                select.add(c);
                c = new CondExpr();
                c.fldNo = 2;
                c.type1 = new AttrType(AttrType.attrSymbol);
                c.op = new AttrOperator(AttrOperator.aopLT);
                c.type2 = new AttrType(AttrType.attrString);
                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                max_c = words[6].substring(0, words[6].length() - 1);
                char_max = (int) max_c.charAt(max_c.length() - 1);
                char_max++;
                max_c = max_c.substring(0, max_c.length() - 1) + Character.toString((char) char_max);
                c.operand2.string = max_c;
                select.add(c);
                if (words[7].charAt(0) == '[') {
                    // valFilter = words[8] + words[9];
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
                    max_c = words[8].substring(0, words[8].length() - 1);
                    char_max = (int) max_c.charAt(max_c.length() - 1);
                    char_max++;
                    max_c = max_c.substring(0, max_c.length() - 1) + Character.toString((char) char_max);
                    c.operand2.string = max_c;
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
            } else {
                colFilter = words[5];
                if (!colFilter.equals("*")) {
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
                    // valFilter = words[7] + words[8];
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
                    max_c = words[7].substring(0, words[7].length() - 1);
                    char_max = (int) max_c.charAt(max_c.length() - 1);
                    char_max++;
                    max_c = max_c.substring(0, max_c.length() - 1) + Character.toString((char) char_max);
                    c.operand2.string = max_c;
                    select.add(c);
                    bufpage = Integer.parseInt(words[8]);
                } else {
                    valFilter = words[6];
                    if (!valFilter.equals("*")) {
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
        } else {
            rowFilter = words[3];
            if (!rowFilter.equals("*")) {
                CondExpr c;
                c = new CondExpr();
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
                c = new CondExpr();
                c.fldNo = 2;
                c.type1 = new AttrType(AttrType.attrSymbol);
                c.op = new AttrOperator(AttrOperator.aopGT);
                c.type2 = new AttrType(AttrType.attrString);
                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                c.operand2.string = words[4].substring(1, words[4].length() - 1);
                select.add(c);
                c = new CondExpr();
                c.fldNo = 2;
                c.type1 = new AttrType(AttrType.attrSymbol);
                c.op = new AttrOperator(AttrOperator.aopLT);
                c.type2 = new AttrType(AttrType.attrString);
                c.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                max_c = words[5].substring(0, words[5].length() - 1);
                char_max = (int) max_c.charAt(max_c.length() - 1);
                char_max++;
                max_c = max_c.substring(0, max_c.length() - 1) + Character.toString((char) char_max);
                c.operand2.string = max_c;
                select.add(c);
                if (words[6].charAt(0) == '[') {

                    // valFilter = words[7] + words[8];
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
                    max_c = words[7].substring(0, words[7].length() - 1);
                    char_max = (int) max_c.charAt(max_c.length() - 1);
                    char_max++;
                    max_c = max_c.substring(0, max_c.length() - 1) + Character.toString((char) char_max);
                    c.operand2.string = max_c;
                    select.add(c);
                    bufpage = Integer.parseInt(words[8]);
                } else {
                    valFilter = words[6];
                    if (!valFilter.equals("*")) {
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
            } else {
                colFilter = words[4];
                if (!colFilter.equals("*")) {
                    CondExpr c;
                    c = new CondExpr();
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
                    c = new CondExpr();
                    c.fldNo = 4;
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
                    max_c = words[6].substring(0, words[6].length() - 1);
                    char_max = (int) max_c.charAt(max_c.length() - 1);
                    char_max++;
                    max_c = max_c.substring(0, max_c.length() - 1) + Character.toString((char) char_max);
                    c.operand2.string = max_c;
                    select.add(c);
                    bufpage = Integer.parseInt(words[7]);
                } else {
                    valFilter = words[5];
                    if (!valFilter.equals("*")) {
                        CondExpr c;
                        c = new CondExpr();
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
        CondExpr[] newSel = new CondExpr[select.size() + 1];
        for (int i = 0; i < select.size(); i++) {
            newSel[i] = select.get(i);
        }
        select.clear();
        newSel[newSel.length - 1] = null;
        query(dbname, order, newSel, bufpage);
    }

    public static void mapInsertDriver(String[] words)
    throws Exception {
        int type = Integer.parseInt(words[5]);
        String dbname = words[6];
        String rl = words[1];
        String cl = words[2];
        String val = words[3];
        int ts = Integer.parseInt(words[4]);
        int numbf = Integer.parseInt(words[7]);

        updateNumbuf(numbf);
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

        map.setHdr(new short[] { 32, 32, 32 });

        f.mapInsert(map.getMapByteArray(), dbname, type);
    }

    public static void getCountDriver(String[] words) throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException, GetFileEntryException,
    PinPageException, ConstructPageException, HFDiskMgrException, HFBufMgrException, HFException,
    InvalidSlotNumberException, NumberFormatException, HashOperationException, PageUnpinnedException,
    PagePinnedException, PageNotFoundException, BufMgrException {
        updateNumbuf(Integer.parseInt(words[1]));
        ArrayList<String> file_list = SystemDefs.JavabaseDB.get_all_files();
        for (String file : file_list) {
            if(file.startsWith("btree")) continue;
            f = new bigt(file);
            System.out.println("Map count in "+file+": "+f.getMapCnt()+ "\nDistinct row count in "+file+": "+f.getRowCnt()+"\nDistinct column count in "+file+": "+f.getColumnCnt());
        }
    }

    public static void rowSortDriver(String[] words) throws UnknowAttrType, LowMemException, JoinsException, Exception {
        String inbtname = words[1];
        String outbtname = words[2];
        String colname = words[3];
        int order = Integer.parseInt(words[4]);
        int numbf = Integer.parseInt(words[5]);
        rowSort(inbtname, outbtname, colname, order, numbf);
    }

    public static void rowSort(String inbtname, String outbtname, String colname, int order, int numbf)
    throws UnknowAttrType, LowMemException, JoinsException, Exception {
        updateNumbuf(numbf);
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
        Sort s = new Sort(fs, 1, new MapOrder(order), 32, numbf, 1, colname, inbtname);
        while(true){
            Map m = s.get_next();
            if(m == null) break;
            outBigt.insertMap(m.getMapByteArray());
            m.print();
        }
        s.close();
    }

    public static boolean batchInsert(String dbFileName, int type, String filepath, int numbf) throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException,
    InvalidSelectionException, IOException, UnknownKeyTypeException, GetFileEntryException,
    ConstructPageException, AddFileEntryException, IteratorException, HashEntryNotFoundException,
    InvalidFrameNumberException, PageUnpinnedException, ReplacerException, HFDiskMgrException,
    HFBufMgrException, HFException, HashOperationException, PagePinnedException, PageNotFoundException,
    BufMgrException, InvalidSlotNumberException, KeyTooLongException, KeyNotMatchException,
    LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException,
    NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, LeafDeleteException,
    InsertException {
        updateNumbuf(numbf);
        f = new bigt(dbFileName);
        f.batchInsert(filepath, type, dbFileName, numbf);
        // batchinsert /home/kaushal/DBMSI/Phase2/project2_testdata2.csv 1 abd 500
        // batchinsert /home/kaushal/DBMSI/Phase2/project2_testdata4.csv 1 bd 500
        // rowjoin abd bd Camel abc 100
        return true;
    }

    public static boolean query(String filename, int order, CondExpr[] select, int numbuf)
    throws LowMemException, Exception {
        updateNumbuf(numbuf);
        PCounter.initialize();
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
        Sort s = new Sort(s_sizes, fileScan, order, new MapOrder(MapOrder.Ascending), 32, numbuf, order);
        
        
        Map map = new Map();
        
        System.out.println();
        while(true){
            map = s.get_next();
            if(map == null){
                break;
            }
            map.print();
        }
        try {
            // fileScan.close();
            s.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println();
        return true;
    }

    public static void updateNumbuf(int numbf) throws HashOperationException, PageUnpinnedException,
    PagePinnedException, PageNotFoundException, BufMgrException, IOException {
        SystemDefs.JavabaseBM.forcedFlush();
        new SystemDefs(fpath+"bigdata", 0, numbf+50, "Clock");
    }

    public static void counterReInit() {
        System.out.println("Read Counter for this operation: "+ PCounter.rcounter);
        System.out.println("Write Counter for this operation: "+ PCounter.wcounter);
        PCounter.initialize();
    }
}