package programs;

import diskmgr.*;
import global.*;
import java.util.Scanner;

public class DBMSI {
    public static String DBFILEPATH = GlobalConst.DBFILEPATH;
    public static int DATABASE_PAGES = GlobalConst.MINIBASE_DB_SIZE;
    public static int NUMBUF = GlobalConst.NUMBUF;
    public static TestDriver testDriver;

    // Main Starting point for the implementation of Phase 3
    public static void main(String[] args) {

        PCounter.initialize();
        Scanner sc = new Scanner(System.in);

        new SystemDefs(DBFILEPATH, DATABASE_PAGES, NUMBUF, "Clock");
        boolean quit = false;

        // Query Parsing
        try {
            do {
                System.out.print(">> ");
                String que = sc.nextLine();
                String[] words = que.split("\\s+");
                if (words[0].equals("batchinsert")) {
                    batchInsertDriver(words);
                } else if (words[0].equals("query")) {
                    queryDriver(words);
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
                TestDriver.counterReInit();
            } while (!quit);
        } catch (Exception e) {
            // System.out.println(e.printStackTrace());
            e.printStackTrace();
        }

        sc.close();
    }

    /*
    TestDriver: Super abstract class for all queries
    
    subclasses with command line instruction:
    MapInsert.java: to insert single map
    mapinsert RowLabel ColLabel Value Timestamp type bigtname numbuf

    BatchInsert.java: to insert batch of data
    batchinsert filename type bigtname numbuf

    Query.java: to query the data from the given bigt
    query bigtname order RowFilter ColFilter ValFilter numbuf

    RowJoin.java: to join two rows in bigt
    rowjoin leftbigt rightbigt ColFilter outbigtname numbuf

    RowSort.java: to sort the data according to the given type
    rowsort bigtname outbigtname ColFiletr 

    getCounts.java: to clear out buffrts
    getCounts numbuf
    */

    public static void queryDriver(String[] words) {
        testDriver = new Query();
        testDriver.performCmd(words);
    }

    public static void batchInsertDriver(String[] words) {
        testDriver = new BatchInsert();
        testDriver.performCmd(words);
    }

    public static void mapInsertDriver(String[] words) {
        testDriver = new MapInsert();
        testDriver.performCmd(words);
    }

    public static void getCountDriver(String[] words) {
        testDriver = new getCounts();
        testDriver.performCmd(words);
    }

    public static void rowSortDriver(String[] words) {
        testDriver = new RowSort();
        testDriver.performCmd(words);
    }

    public static void rowJoinDriver(String[] words) {
        testDriver = new RowJoin();
        testDriver.performCmd(words);
    }
}