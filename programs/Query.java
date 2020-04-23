package programs;

import java.util.ArrayList;

import iterator.*;
import global.*;
import BigT.*;
/**
 * Querying the bigt
 * Generates condExpr on FileScan with given parameter.
 */
public class Query extends TestDriver {

    @Override
    public void performCmd(String[] words) {
        // TODO Auto-generated method stub
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
        try {
            query(dbname, order, newSel, bufpage);
        } catch (Exception e) {
            // TODO Auto-generated catch block
        }
    }
    
    public boolean query(String filename, int order, CondExpr[] select, int numbuf)
    throws LowMemException, Exception {
        updateNumbuf(numbuf);
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

}