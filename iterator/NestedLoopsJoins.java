package iterator;
   

import BigT.*;
import global.*;
import bufmgr.*;
import index.*;
import java.io.*;
/** 
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */

public class NestedLoopsJoins  extends Iterator 
{
  private AttrType      _in1[],  _in2[];
  private   int        in1_len, in2_len;
  private   Iterator  innerx;
  private Stream outerx, outx2, finalOp = null;
  private   short t2_str_sizescopy[];
  private   CondExpr OutputFilter[];
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean        done,         // Is the join complete
                      get_from_outer;                 // if TRUE, a tuple is got from outer
  private   Map     outer_tuple, inner_tuple;
  private   Map     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  private   bigt  hf;
  private   Sort        s1;
  private   Sort        s2;
  private   String      currentRow = "None";
  private   boolean     isJoin = false;
  private   int         maxTimeStamp = 0;
  private   String      _colname, rightbtname, leftbtname;
  private   boolean     isFirstTime = true;
  private   bigt        btTopLeft, btTopRight, T3, T4, T5, finalOutput, leftb;
  private   FldSpec[]   proj_list;

  
  
  /**
   * constructor Initialize the two relations which are joined, including relation
   * type,
   * 
   * @param in1          Array containing field types of R.
   * @param len_in1      # of columns in R.
   * @param t1_str_sizes shows the length of the string fields.
   * @param in2          Array containing field types of S
   * @param len_in2      # of columns in S
   * @param t2_str_sizes shows the length of the string fields.
   * @param amt_of_mem   IN PAGES
   * @param am1          access method for left i/p to join
   * @param relationName access hfapfile for right i/p to join
   * @param outFilter    select expressions
   * @param rightFilter  reference to filter applied on right i/p
   * @param proj_list    shows what input fields go where in the output tuple
   * @param n_out_flds   number of outer relation fileds
   * @exception IOException         some I/O fault
   * @exception NestedLoopException exception from this class
   * @throws SortException
   * @throws InvalidSlotNumberException
   * @throws HFException
   * @throws HFBufMgrException
   * @throws HFDiskMgrException
   * @throws InvalidRelation
   * @throws MapUtilsException
   * @throws FileScanException
   * @throws InvalidTupleSizeException
   */
  public NestedLoopsJoins(String colname, int amt_of_mem, Stream leftStream, String am2_s, CondExpr outFilter[],
      CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds)
      throws IOException, NestedLoopException, SortException, HFDiskMgrException, HFBufMgrException, HFException,
      InvalidSlotNumberException, FileScanException, MapUtilsException, InvalidRelation, InvalidTupleSizeException {
    
    _colname = colname;
    AttrType[] in1 = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    AttrType[] in2 = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    _in1 = new AttrType[in1.length];
    _in2 = new AttrType[in2.length];
    System.arraycopy(in1,0,_in1,0,in1.length);
    System.arraycopy(in2,0,_in2,0,in2.length);
    int len_in1 = 4;
    int len_in2 = 4;
    in1_len = len_in1;
    in2_len = len_in2;
    finalOutput = new bigt("finalOutput");
    short[] t1_str_sizes = {32,32,32};
    short[] t2_str_sizes = {32,32,32};
    rightbtname = am2_s;
    leftbtname = leftStream.getBigtName();
    leftb = new bigt(leftbtname);
    // System.out.println(leftStream.getBigtName());
    outerx = leftb.openStream();
    this.proj_list = proj_list;
    innerx = new FileScan(am2_s, 0, t1_str_sizes, 4, proj_list, null);
    t2_str_sizescopy =  t2_str_sizes;
    inner_tuple = new Map();
    Jtuple = new Map();
    OutputFilter = outFilter;
    RightFilter  = rightFilter;
    
    n_buf_pgs    = amt_of_mem;
    done  = false;
    get_from_outer = true;
    
    AttrType[] Jtypes = new AttrType[n_out_flds];
    short[]    t_size;
    
    perm_mat = proj_list;
    nOutFlds = n_out_flds;
    try {
      t_size = MapUtils.setup_op_tuple(Jtuple, Jtypes, t1_str_sizes, t2_str_sizes, proj_list, nOutFlds);
    }catch (MapUtilsException e){
      throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
    }
    
    
    
    s2 = new Sort(new short[]{32,32,32}, innerx, 1, new MapOrder(MapOrder.Ascending), 32, n_buf_pgs/2, 1);
    s1 = new Sort(outerx, 1, new MapOrder(MapOrder.Ascending), 32, n_buf_pgs/2, 1);
  }
  
  /**  
   * RowJoin: first takes the highest timestamp with given colname for all rows in both bigt.
   * and then matches them accordingly.
   * btTopLeft: outer table most recent map with column
   * btTopRight: inner table most recent map with column
   *@return The joined tuple is returned
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception TupleUtilsException exception from using tuple utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions
  */
  public Map get_next() throws IOException,
    JoinsException ,
    IndexException,
    InvalidTupleSizeException,
    InvalidTypeException, 
    PageNotReadException,
    MapUtilsException, 
    PredEvalException,
    SortException,
    LowMemException,
    UnknowAttrType,
    UnknownKeyTypeException,
    Exception
  {
    // This is a DUMBEST form of a join, not making use of any key information...
    Map l1 = new Map();
    Map l2 = new Map();
    
    Map nextMap = null;
    
    Map m;
    if(isFirstTime){
      isFirstTime = false;

      btTopLeft = new bigt("btTopLeft");
      btTopRight = new bigt("btTopRight");
      T3 = new bigt("T3");
      T4 = new bigt("T4");
      T5 = new bigt("T5");

      // extracting table btTopLeft

      while(true){
        if(nextMap != null){
          if(nextMap.getColumnLabel().equals(_colname) && nextMap.getTimeStamp()>maxTimeStamp){
            l1=new Map(nextMap.getMapByteArray(), 0);
            l1.mapSetup();
            isJoin = true;
            maxTimeStamp = nextMap.getTimeStamp();
          }
          nextMap = null;
          continue;
        }
        m = s1.get_next();
        if(m == null){
          if(isJoin){
            btTopLeft.insertMap(l1.getMapByteArray());
            isJoin = false;
          }
          break;
        }
        if(currentRow.equals("None")){
          currentRow = m.getRowLabel();
          if(m.getColumnLabel().equals(_colname) && m.getTimeStamp()>maxTimeStamp){
            l1=new Map(m.getMapByteArray(), 0);
            l1.mapSetup();
            maxTimeStamp = m.getTimeStamp();
            isJoin = true;
          }
        }else if(currentRow.equals(m.getRowLabel())){
          if(m.getColumnLabel().equals(_colname) && m.getTimeStamp()>maxTimeStamp){
            l1=new Map(m.getMapByteArray(), 0);
            l1.mapSetup();
            maxTimeStamp = m.getTimeStamp();
            isJoin = true;
          }
        }else {
          nextMap=new Map(m.getMapByteArray(), 0);
          nextMap.mapSetup();
          if(isJoin){
            btTopLeft.insertMap(l1.getMapByteArray());
            isJoin = false;
          }
          l1 = new Map();
          currentRow = m.getRowLabel();
          maxTimeStamp = 0;
        }
      }
      isJoin = false;
      currentRow = "None";
      maxTimeStamp = 0;

      // extracting table btTopRight

      while(true){
        if(nextMap != null){
          if(nextMap.getColumnLabel().equals(_colname) && nextMap.getTimeStamp()>maxTimeStamp){
            l2=new Map(nextMap.getMapByteArray(), 0);
            l2.mapSetup();
            isJoin = true;
            maxTimeStamp = nextMap.getTimeStamp();
          }
          nextMap = null;
          continue;
        }
        m = s2.get_next();
        if(m == null){
          if(isJoin){
            btTopRight.insertMap(l2.getMapByteArray());
            isJoin = false;
            
          }
          break;
        }
        if(currentRow.equals("None")){
          currentRow = m.getRowLabel();
          if(m.getColumnLabel().equals(_colname) && m.getTimeStamp()>maxTimeStamp){
            l2=new Map(m.getMapByteArray(), 0);
            l2.mapSetup();
            maxTimeStamp = m.getTimeStamp();
            isJoin = true;
          }
        }else if(currentRow.equals(m.getRowLabel())){
          if(m.getColumnLabel().equals(_colname) && m.getTimeStamp()>maxTimeStamp){
            l2=new Map(m.getMapByteArray(), 0);
            l2.mapSetup();
            maxTimeStamp = m.getTimeStamp();
            isJoin = true;
          }
        }else {
          nextMap=new Map(m.getMapByteArray(), 0);
          nextMap.mapSetup();
          if(isJoin){
            btTopRight.insertMap(l2.getMapByteArray());
            isJoin = false;
          }
          l2 = new Map();
          currentRow = m.getRowLabel();
          maxTimeStamp = 0;
        }
      }

      MID topRightMid = new MID();
      MID topLeftMid = new MID();
      
      //assume that we have topleft and top right at this point

      // join top left, top right based on value 
      
      Stream topLeftStream = btTopLeft.openStream();
      Stream topRightStream = btTopRight.openStream();
      Boolean topLeftDone = false;
      Boolean topRightDone = false;

      outern: while(!topLeftDone){
        Map topLeftmap = topLeftStream.getNext(topLeftMid);
        if (topLeftmap == null) {
          topLeftDone = true;
          break outern;
        }
        
        topRightDone = false;
        topRightStream = btTopRight.openStream();
        innern: while(!topRightDone){
            
          Map topRightMap = topRightStream.getNext(topRightMid);
          if(topRightMap == null){
            topRightDone = true;
            break innern;
          }
          // we assume that we have top left and top right at this point
          
          if(topLeftmap.getValue().equalsIgnoreCase(topRightMap.getValue())){
            // add left map to T3 
            T3.insertMap(topLeftmap.getMapByteArray());
            // add right map to T4
            T4.insertMap(topRightMap.getMapByteArray());
          }

        }
      }
      
      MID t3Mid = new MID();
      MID t4Mid = new MID();
      Stream t3Stream = T3.openStream();
      Stream t4Stream = T4.openStream();
      Boolean t3Done = false;
      Boolean t4Done = false;
      outer2n: while(!t3Done){
        Map t3Map = t3Stream.getNext(t3Mid);
        if (t3Map == null){
          t3Done = true;
          break outer2n;
        }
        t4Done = false;
        t4Stream = T4.openStream();
        inner2n: while(!t4Done){
          
          Map t4Map =  t4Stream.getNext(t4Mid);
          if (t4Map == null){
            t4Done = true;
            break inner2n;
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
            T5.insertMap(tempMap.getMapByteArray());
            // T5_2.add(tempMap);
              
          }
        }
      }

      Boolean t5Done = false;
      Boolean leftDone = false;
      Stream t5Stream;
      MID t5Mid = new MID();
      MID leftMid = new MID();
      outerx = leftb.openStream();
      innerx = new FileScan(rightbtname, 0, new short[]{32,32,32}, 4 , proj_list, null);
      outer3: while(!leftDone){
        Map leftMap = outerx.getNext(leftMid);
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
            if(leftMap.getColumnLabel().equalsIgnoreCase(_colname)){
              Map tempMap2 = new Map();
              tempMap2.setHdr(new short[] { 32, 32, 32 });
              tempMap2.setRowLabel(t5Map.getValue());
              tempMap2.setColumnLabel(leftMap.getColumnLabel());
              tempMap2.setTimeStamp(leftMap.getTimeStamp());
              tempMap2.setValue(leftMap.getValue());
              // our map will be of the form [R1, R2, 1, R1:R2]
              tempMap2.mapSetup();
              finalOutput.insertMap(tempMap2.getMapByteArray());
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
              finalOutput.insertMap(tempMap2.getMapByteArray());
            }
          }
        }
      }


      //right table join with T5 fld 2, // we have right bigtname
      t5Done = false;
      Boolean rightDone = false;
      t5Stream = T5.openStream();
      t5Mid = new MID();
      
      outer4: while(!t5Done){
        Map t5Map = t5Stream.getNext(t5Mid);
        if(t5Map == null){
          t5Done = true;
          break outer4;
        }
        rightDone = false;
        innerx = new FileScan(rightbtname, 0, new short[]{32,32,32}, 4 , proj_list, null);
        inner4: while(!rightDone){
          Map rightMap = innerx.get_next();
          if(rightMap == null){
            rightDone = true;
            innerx.close();
            break inner4;
          }

          if(t5Map.getColumnLabel().equalsIgnoreCase(rightMap.getRowLabel())){
            if(rightMap.getColumnLabel().equalsIgnoreCase(_colname)){
              Map tempMap3 = new Map();
              tempMap3.setHdr(new short[] { 32, 32, 32 });
              tempMap3.setRowLabel(t5Map.getValue());
              tempMap3.setColumnLabel(rightMap.getColumnLabel());
              tempMap3.setTimeStamp(rightMap.getTimeStamp());
              tempMap3.setValue(rightMap.getValue());
              // our map will be of the form [R1, R2, 1, R1:R2]
              tempMap3.mapSetup();
              finalOutput.insertMap(tempMap3.getMapByteArray());
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
              finalOutput.insertMap(tempMap3.getMapByteArray());
            }
          }
        }
      }
      btTopLeft.deleteBigt();
      btTopRight.deleteBigt();
      T3.deleteBigt();
      T4.deleteBigt();
      T5.deleteBigt();
      finalOp = finalOutput.openStream();
      return finalOp.getNext(new MID());
    }
    else{
      return finalOp.getNext(new MID());
    }
  } 
 
  /**
   * implement the abstract method close() from super class Iterator
   *to finish cleaning up
   *@exception IOException I/O error from lower layers
   *@exception JoinsException join error from lower layers
   *@exception IndexException index access error 
   */
  public void close() throws JoinsException, IOException,IndexException {
    if (!closeFlag) {
      try {
        innerx.close();
        s1.close();
        s2.close();
      }catch (Exception e) {
        throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
      }
      closeFlag = true;
    }
  }
}