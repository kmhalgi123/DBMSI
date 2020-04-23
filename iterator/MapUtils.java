package iterator;


import BigT.*;
import global.*;
import java.io.*;

/**
 *some useful method when processing Tuple 
 */
public class MapUtils
{
  
  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple.
   *@param    t2        another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.
   *@param    t2_fld_no the field numbers in the tuples to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,                              
   */
  public static int CompareMapWithMap(Map  m1, Map m2, int fld_no) throws IOException, UnknowAttrType, MapUtilsException {
    int   t1_i,  t2_i;
    String t1_s, t2_s;
      
    switch (fld_no) {
      case 1:                // Compare two integers.
        try {
          t1_s = m1.getRowLabel();
          t2_s = m2.getRowLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        if (t1_s.compareTo( t2_s)<0)return -1;
        return 0;
        
      case 2:                // Compare two floats
        try {
          t1_s = m1.getColumnLabel();
          t2_s = m2.getColumnLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        if (t1_s.compareTo( t2_s)<0)return -1;
        return 0;
        
      case 3:                // Compare two strings
        try {
          t1_i = m1.getTimeStamp();
          t2_i = m2.getTimeStamp();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        if (t1_i == t2_i) return  0;
        if (t1_i <  t2_i) return -1;
        if (t1_i >  t2_i) return  1;
        return 0;

      case 4:                // Compare two floats
        try {
          t1_s = m1.getValue();
          t2_s = m2.getValue();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        if (t1_s.compareTo( t2_s)<0)return -1;
        return 0;
      default:
        
        throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
        
    }
  }
  
  public static int customMapCompare(Map m1, Map m2, int customCompareType) throws MapUtilsException, UnknowAttrType {
    int   t1_i,  t2_i;
    String t1_s, t2_s;
    
    switch (customCompareType) {
      case 1:
        try {
          t1_s = m1.getRowLabel();
          t2_s = m2.getRowLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        else{
          try {
            t1_s = m1.getColumnLabel();
            t2_s = m2.getColumnLabel();
          }catch (Exception e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
          }
          if(t1_s.compareTo( t2_s)>0)return 1;
          else if (t1_s.compareTo( t2_s)<0)return -1;
          else{
            try {
              t1_i = m1.getTimeStamp();
              t2_i = m2.getTimeStamp();
            }catch (Exception e){
              throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
            }
            if (t1_i == t2_i) return  0;
            if (t1_i <  t2_i) return -1;
            if (t1_i >  t2_i) return  1;
            return 0;
          }
        }
      case 2:
        try {
          t1_s = m1.getColumnLabel();
          t2_s = m2.getColumnLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        else{
          try {
            t1_s = m1.getRowLabel();
            t2_s = m2.getRowLabel();
          }catch (Exception e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
          }
          if(t1_s.compareTo( t2_s)>0)return 1;
          else if (t1_s.compareTo( t2_s)<0)return -1;
          else{
            try {
              t1_i = m1.getTimeStamp();
              t2_i = m2.getTimeStamp();
            }catch (Exception e){
              throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
            }
            if (t1_i == t2_i) return  0;
            if (t1_i <  t2_i) return -1;
            if (t1_i >  t2_i) return  1;
            return 0;
          }
        }
      case 3:
        try {
          t1_s = m1.getRowLabel();
          t2_s = m2.getRowLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        else{
          try {
            t1_i = m1.getTimeStamp();
            t2_i = m2.getTimeStamp();
          }catch (Exception e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
          }
          if (t1_i == t2_i) return  0;
          if (t1_i <  t2_i) return -1;
          if (t1_i >  t2_i) return  1;
          return 0;
        }
      case 4:
        try {
          t1_s = m1.getColumnLabel();
          t2_s = m2.getColumnLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        else{
          try {
            t1_i = m1.getTimeStamp();
            t2_i = m2.getTimeStamp();
          }catch (Exception e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
          }
          if (t1_i == t2_i) return  0;
          if (t1_i <  t2_i) return -1;
          if (t1_i >  t2_i) return  1;
          return 0;
        }
      case 5:
        try {
          t1_s = m1.getColumnLabel();
          t2_s = m2.getColumnLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        else{
          try {
            t1_i = m1.getTimeStamp();
            t2_i = m2.getTimeStamp();
          }catch (Exception e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
          }
          if (t1_i == t2_i) return  0;
          if (t1_i <  t2_i) return -1;
          if (t1_i >  t2_i) return  1;
          return 0;
        }
      case 6:
        try {
          t1_s = m1.getRowLabel();
          t2_s = m2.getRowLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        else{
          try {
            t1_s = m1.getValue();
            t2_s = m2.getValue();
          }catch (Exception e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
          }
          if(t1_s.compareTo( t2_s)>0)return 1;
          else if (t1_s.compareTo( t2_s)<0)return -1;
          return 0;
        }
      case 7:
        try {
          t1_s = m1.getRowLabel();
          t2_s = m2.getRowLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        return 0;
      case 8:
        try {
          t1_s = m1.getColumnLabel();
          t2_s = m2.getColumnLabel();
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        
        // Now handle the special case that is posed by the max_values for strings...
        if(t1_s.compareTo( t2_s)>0)return 1;
        else if (t1_s.compareTo( t2_s)<0)return -1;
        return 0;
      default:
        throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
    }
  }
  
  /**
   * This function  compares  tuple1 with another tuple2 whose
   * field number is same as the tuple1
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple
   *@param    value     another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.  
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,  
   *@exception UnknowAttrType don't know the attribute type   
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class   
   */            
  public static int CompareMapWithValue(Map m1, Map value, int fld_no) throws IOException, UnknowAttrType, MapUtilsException
  {
    value.mapSetup();
    return CompareMapWithMap(m1, value, fld_no);
  }
  
  /**
   *This function Compares two Tuple inn all fields 
   * @param t1 the first tuple
   * @param t2 the secocnd tuple
   * @param type[] the field types
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */            
  
  public static boolean Equal(Map m1, Map m2) throws IOException,UnknowAttrType,MapUtilsException
  {
    for (int i = 1; i <= 4; i++)
    if (CompareMapWithMap(m1, m2, i) != 0)
      return false;
    return true;
  }
  
  /**
   *get the string specified by the field number
   *@param tuple the tuple 
   *@param fidno the field number
   *@return the content of the field number
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */
  public static String Value(Map map, int fldno) throws IOException, MapUtilsException
  {
    String temp;
    try{
      temp = map.getStrFld(fldno);
    }catch (Exception e){
      throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
    }
    return temp;
  }
  
 
  /**
   *set up a tuple in specified field from a tuple
   *@param value the tuple to be set 
   *@param tuple the given tuple
   *@param fld_no the field number
   *@param fldType the tuple attr type
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */  
  public static void SetValue(Map value, Map map, int fld_no) throws IOException, UnknowAttrType, MapUtilsException
  {
      
    switch (fld_no) {
      case 1:
        try {
          value.setRowLabel(map.getRowLabel());
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      case 2:
        try {
          value.setColumnLabel(map.getColumnLabel());
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      case 3:
        try {
          value.setTimeStamp(map.getTimeStamp());
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      case 4:
        try {
          value.setValue(map.getValue());
        }catch (Exception e){
          throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        break;
      default:
        throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
        
	  }
      
    return;
  }
  
  
  /**
   *set up the Jtuple's attrtype, string size,field number for using join
   *@param Jtuple  reference to an actual tuple  - no memory has been malloced
   *@param res_attrs  attributes type of result tuple
   *@param in1  array of the attributes of the tuple (ok)
   *@param len_in1  num of attributes of in1
   *@param in2  array of the attributes of the tuple (ok)
   *@param len_in2  num of attributes of in2
   *@param t1_str_sizes shows the length of the string fields in S
   *@param t2_str_sizes shows the length of the string fields in R
   *@param proj_list shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */
  public static short[] setup_op_tuple(Map Jtuple, AttrType[] res_attrs, 
    short t1_str_sizes[], 
    short t2_str_sizes[], 
    FldSpec proj_list[], int nOutFlds)
    throws IOException, MapUtilsException
    {
      short [] sizesT1 = new short [4];
      short [] sizesT2 = new short [4];
      AttrType in1[] = {new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrString)};

      AttrType in2[] = {new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrString)};
      int i, count = 0;
      
      for (i = 0; i < 4; i++)
        if (in1[i].attrType == AttrType.attrString)
	        sizesT1[i] = t1_str_sizes[count++];
      
      for (count = 0, i = 0; i < 4; i++)
        if (in2[i].attrType == AttrType.attrString)
          sizesT2[i] = t2_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
      {
        if (proj_list[i].relation.key == RelSpec.outer)
          res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
        else if (proj_list[i].relation.key == RelSpec.innerRel)
          res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
      }
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
      {
        if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
      }
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++)
      {
        if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
        else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
      }
      try {
	      Jtuple.setHdr(res_str_sizes);
      }catch (Exception e){
	      throw new MapUtilsException(e,"setHdr() failed");
      }
      return res_str_sizes;
    }
  
 
   /**
   *set up the Jtuple's attrtype, string size,field number for using project
   *@param Jtuple  reference to an actual tuple  - no memory has been malloced
   *@param res_attrs  attributes type of result tuple
   *@param in1  array of the attributes of the tuple (ok)
   *@param len_in1  num of attributes of in1
   *@param t1_str_sizes shows the length of the string fields in S
   *@param proj_list shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */

  public static short[] setup_op_tuple(Map Jtuple, AttrType res_attrs[],
    short t1_str_sizes[], 
    FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   MapUtilsException, 
	   InvalidRelation
    {

      AttrType in1[] = {new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrString)};
      short [] sizesT1 = new short [4];
      int i, count = 0;
      
      for (i = 0; i < 4; i++)
        if (in1[i].attrType == AttrType.attrString)
	        sizesT1[i] = t1_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
      {
        if (proj_list[i].relation.key == RelSpec.outer) 
          res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
        
        else throw new InvalidRelation("Invalid relation -innerRel");
      }
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
      {
        if (proj_list[i].relation.key == RelSpec.outer
            && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
          n_strs++;
      }
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++) {
        if (proj_list[i].relation.key ==RelSpec.outer
            && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
          res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
      }
     
      try {
	      Jtuple.setHdr(res_str_sizes);
      }catch (Exception e){
	      throw new MapUtilsException(e,"setHdr() failed");
      } 
      return res_str_sizes;
    }
}




