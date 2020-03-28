package global;

import java.sql.Time;

/** 
 * Enumeration class for IndexType
 * 
 */

public class IndexType {

  public static final int No_Index = 1;
  public static final int Row_Label_Index = 2;
  public static final int Column_Label_Index = 3;
  public static final int Column_Row_Label_Index = 4;
  public static final int Row_Label_Value_Index = 5;
  public static final int Timestamp_Index = 6;

  public int indexType;

  /** 
   * IndexType Constructor
   * <br>
   * An index type can be defined as 
   * <ul>
   * <li>   IndexType indexType = new IndexType(IndexType.Hash);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (indexType.indexType == IndexType.Hash) ....
   * </ul>
   *
   * @param _indexType The possible types of index
   */

  public IndexType (int _indexType) {
    indexType = _indexType;
  }

    public String toString() {

    switch (indexType) {
    case No_Index:
      return "No Index";
    case Row_Label_Index:
      return "Row Label Index";
    case Column_Label_Index:
      return "Column Label Index";
    case Column_Row_Label_Index:
      return "Column and Row Label Index";
    case Row_Label_Value_Index:
      return "Row Label and Value Index";
    case Timestamp_Index:
      return "Timestamp Index";
      
    }
    return ("Unexpected IndexType " + indexType);
  }
}