package programs;

import java.util.ArrayList;
import BigT.*;
import global.*;

/*
    A java code to get counts of every bigt and overall database
    three methods have been used.
    getMapCnt(): returns maps in bigt
    getRowCnt(): returns Arraylisy of distinct rows
    getColCnt(): returns Arraylisy of distinct columns
*/

public class getCounts extends TestDriver {

    @Override
    public void performCmd(String[] words) {

        updateNumbuf(Integer.parseInt(words[1]));
        ArrayList<String> file_list;
        ArrayList<String> rowLabels = new ArrayList<>();
        ArrayList<String> colLabels = new ArrayList<>();
        int mapcnt=0;
        try {
            file_list = SystemDefs.JavabaseDB.get_all_files();
            for (String file : file_list) {
                if(file.startsWith("btree") || file.startsWith("tempHeapFile")) continue;
                bigt f = new bigt(file);
                ArrayList<String> this_row_labels = f.getRowCnt();
                ArrayList<String> this_col_labels = f.getColumnCnt();
                for (String string : this_row_labels) {
                    if(!rowLabels.contains(string)){
                        rowLabels.add(string);
                    }
                }
                for (String string : this_col_labels) {
                    if(!colLabels.contains(string)){
                        colLabels.add(string);
                    }
                }
                int this_file_maps = f.getMapCnt();
                mapcnt += this_file_maps;
                System.out.println("Map count in "+file+": "+this_file_maps+ "\nDistinct row count in "+file+": "+this_row_labels.size()+"\nDistinct column count in "+file+": "+this_col_labels.size());
            }
            System.out.println("Total maps in database: "+mapcnt+"\nTotal distinct rows in databse: "+rowLabels.size()+"\nTotal distinct columns in databse: "+colLabels.size());
        } catch (Exception e){

        }
        
    }

}