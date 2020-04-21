package iterator;

import BigT.*;
import global.*;
import java.io.*;
import java.util.ArrayList;
/**
 * Jtuple has the appropriate types.
 * Jtuple already has its setHdr called to setup its vital stas.
 */

public class Projection
{
  	/**
	 * Tuple t1 and Tuple t2 will be joined, and the result will be stored in Tuple
	 * Jtuple,before calling this mehtod. we know that this two tuple can join in
	 * the common field
	 * 
	 * @param t1         The Tuple will be joined with t2
	 * @param type1[]    The array used to store the each attribute type
	 * @param t2         The Tuple will be joined with t1
	 * @param type2[]    The array used to store the each attribute type
	 * @param Jtuple     the returned Tuple
	 * @param perm_mat[] shows what input fields go where in the output tuple
	 * @param nOutFlds   number of outer relation field
	 * @exception UnknowAttrType                 attrbute type does't match
	 * @exception FieldNumberOutOfBoundException field number exceeds limit
	 * @exception IOException                    some I/O fault
	 * @throws MapUtilsException
	 */
	public static void Join(Map m1, Map m2, Map Jmap, FldSpec perm_mat[], int nOutFlds)
			throws UnknowAttrType, FieldNumberOutOfBoundException, IOException, MapUtilsException
    {
		AttrType[] type1 = {new AttrType(AttrType.attrString),
			new AttrType(AttrType.attrString),
			new AttrType(AttrType.attrInteger),
			new AttrType(AttrType.attrString)};

		AttrType[] type2 = {new AttrType(AttrType.attrString),
			new AttrType(AttrType.attrString),
			new AttrType(AttrType.attrInteger),
			new AttrType(AttrType.attrString)};
      
      	for (int i = 0; i < nOutFlds; i++) {
	  		switch (perm_mat[i].relation.key) {
	    		case RelSpec.outer:        // Field of outer (t1)
					switch (type1[perm_mat[i].offset-1].attrType) {
						case AttrType.attrInteger:
							Jmap.setTimeStamp(m1.getTimeStamp());
							break;
						case AttrType.attrString:
							Jmap.setStrFld(i+1, m1.getStrFld(perm_mat[i].offset));
							break;
						default:
							throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");
					}
	      			break;
	      
				case RelSpec.innerRel:        // Field of inner (t2)
					switch (type2[perm_mat[i].offset-1].attrType)
					{
						case AttrType.attrInteger:
							Jmap.setTimeStamp(m2.getTimeStamp());
							break;
						case AttrType.attrString:
							Jmap.setStrFld(i+1, m2.getStrFld(perm_mat[i].offset));
							break;
						default:
					
							throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");  
					
					}
					break;
				}
			}
      	return;
	}
	
	public static void Join(Map m1, Map m2, ArrayList<Map> output, FldSpec perm_mat[], String colname) throws IOException {
		String rowop = m1.getRowLabel()+":"+m2.getRowLabel();
		if(m1.getColumnLabel().equals(m2.getColumnLabel()) && m1.getColumnLabel().equals(colname)){
			byte[] recbyte = new byte[116];
			Map m = new Map(recbyte, 0);
			m.setHdr(new short[]{32,32,32});
			m.setRowLabel(rowop);
			m.setColumnLabel(colname);
			m.setTimeStamp(m2.getTimeStamp());
			m.setValue(m2.getValue());
			output.add(m);
		}else {
			byte[] recbyte = new byte[116];
			Map m = new Map(recbyte, 0);
			m.setHdr(new short[]{32,32,32});
			m.setRowLabel(rowop);
			m.setColumnLabel(m2.getColumnLabel()+"_right");
			m.setTimeStamp(m2.getTimeStamp());
			m.setValue(m2.getValue());
			output.add(m);
		}
	}
  
  
  
  
  
  	/**
	 * Tuple t1 will be projected the result will be stored in Tuple Jtuple
	 * 
	 * @param t1         The Tuple will be projected
	 * @param type1[]    The array used to store the each attribute type
	 * @param Jtuple     the returned Tuple
	 * @param perm_mat[] shows what input fields go where in the output tuple
	 * @param nOutFlds   number of outer relation field
	 * @exception UnknowAttrType                 attrbute type doesn't match
	 * @exception WrongPermat                    wrong FldSpec argument
	 * @exception FieldNumberOutOfBoundException field number exceeds limit
	 * @exception IOException                    some I/O fault
	 * @throws MapUtilsException
	 */

	public static void Project(Map m1, Map Jmap, FldSpec perm_mat[], int nOutFlds)
			throws UnknowAttrType, WrongPermat, FieldNumberOutOfBoundException, IOException, MapUtilsException
    {
		AttrType[] type1 = {new AttrType(AttrType.attrString),
			new AttrType(AttrType.attrString),
			new AttrType(AttrType.attrInteger),
			new AttrType(AttrType.attrString)};
      
      	for (int i = 0; i < nOutFlds; i++) {
	  		switch (perm_mat[i].relation.key) {
				case RelSpec.outer:      // Field of outer (t1)
					switch (type1[perm_mat[i].offset-1].attrType) {
						case AttrType.attrInteger:
							Jmap.setTimeStamp(m1.getTimeStamp());
							break;
						case AttrType.attrString:
							Jmap.setStrFld(i+1, m1.getStrFld(perm_mat[i].offset));
							break;
						default:
						
							throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull"); 
					
					}
					break;
					
				default:
					
					throw new WrongPermat("something is wrong in perm_mat");
					
			}
		}
		return;
    }
  
}


