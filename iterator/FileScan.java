package iterator;
   

import BigT.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class FileScan extends  Iterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes; 
  private bigt f;
  private Stream scan;
  private Map     tuple1;
  private Map    Jtuple;
  private int        t1_size;
  private int nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;

 

  /**
   *constructor
   *@param file_name heapfile to be opened
   *@param in1[]  array showing what the attributes of the input fields are. 
   *@param s1_sizes[]  shows the length of the string fields.
   *@param len_in1  number of attributes in the input tuple
   *@param n_out_flds  number of fields in the out tuple
   *@param proj_list  shows what input fields go where in the output tuple
   *@param outFilter  select expressions
   *@exception IOException some I/O fault
   *@exception FileScanException exception from this class
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */
  public  FileScan (String  file_name,
    int type,
    short s1_sizes[],
    int n_out_flds,
    FldSpec[] proj_list,
    CondExpr[]  outFilter 
		)
    throws IOException,
	   FileScanException,
	   MapUtilsException, 
	   InvalidRelation
  {
    AttrType in1[] = {new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString)};
    _in1 = in1; 
    in1_len = 4;
    s_sizes = s1_sizes;
    
    Jtuple =  new Map();
    AttrType[] Jtypes = new AttrType[n_out_flds];
    short[]    ts_size;
    ts_size = MapUtils.setup_op_tuple(Jtuple, Jtypes, s1_sizes, proj_list, n_out_flds);
    
    OutputFilter = outFilter;
    perm_mat = proj_list;
    nOutFlds = n_out_flds; 
    tuple1 =  new Map();

    try {
      tuple1.setHdr(s1_sizes);
    }catch (Exception e){
      throw new FileScanException(e, "setHdr() failed");
    }
    t1_size = tuple1.size();
    
    try {
      f = new bigt(file_name);

    }
    catch(Exception e) {
      throw new FileScanException(e, "Create new heapfile failed");
    }
    
    try {
      scan = f.openStream();
    }
    catch(Exception e){
      throw new FileScanException(e, "openScan() failed");
    }
  }
  
  /**
   *@return shows what input fields go where in the output tuple
   */
  public FldSpec[] show()
    {
      return perm_mat;
    }
  
  /**
   * @return the result tuple
   * @exception JoinsException                 some join exception
   * @exception IOException                    I/O errors
   * @exception InvalidTupleSizeException      invalid tuple size
   * @exception InvalidTypeException           tuple type not valid
   * @exception PageNotReadException           exception from lower layer
   * @exception PredEvalException              exception from PredEval class
   * @exception UnknowAttrType                 attribute type unknown
   * @exception FieldNumberOutOfBoundException array out of bounds
   * @exception WrongPermat                    exception for wrong FldSpec
   *                                           argument
   * @throws MapUtilsException
   */
  public Map get_next()
      throws JoinsException, IOException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException,
      PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, MapUtilsException
    {     
      MID rid = new MID();;
      
      while(true) {
	      if((tuple1 =  scan.getNext(rid)) == null) {
	        return null;
	      }
	
        tuple1.mapSetup();
        if (PredEval.Eval(OutputFilter, tuple1, null) == true){
          Projection.Project(tuple1, Jtuple, perm_mat, nOutFlds); 
          return  Jtuple;
        }        
      }
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
  {
    
    if (!closeFlag) {
      scan.closescan();
      closeFlag = true;
    } 
  }
  
}


