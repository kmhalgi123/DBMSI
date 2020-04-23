package programs;

import java.io.*;
import BigT.*;
import btree.*;
import bufmgr.*;
import iterator.*;
import index.*;

/**
 * BatchInsert: To insert a whole batch of the data.
 */

public class BatchInsert extends TestDriver {

    @Override
    public void performCmd(String[] words) {
        String filepath = words[1];
        int type = Integer.parseInt(words[2]);
        String dbname = words[3];
        int numbf = Integer.parseInt(words[4]);
        try {
            batchInsert(dbname, type, filepath, numbf);
        } catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
                | InvalidSelectionException | UnknownKeyTypeException | GetFileEntryException | ConstructPageException
                | AddFileEntryException | IteratorException | HashEntryNotFoundException | InvalidFrameNumberException
                | PageUnpinnedException | ReplacerException | HFDiskMgrException | HFBufMgrException | HFException
                | HashOperationException | PagePinnedException | PageNotFoundException | BufMgrException
                | InvalidSlotNumberException | KeyTooLongException | KeyNotMatchException | LeafInsertRecException
                | IndexInsertRecException | UnpinPageException | PinPageException | NodeNotMatchException
                | ConvertException | DeleteRecException | IndexSearchException | LeafDeleteException | InsertException
                | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 
     * @param dbFileName: bigt name
     * @param type: storage fashion
     * @param filepath: file of records
     * @param numbf: number of buffers
     * @return
     * @throws IndexException
     * @throws InvalidTypeException
     * @throws InvalidTupleSizeException
     * @throws UnknownIndexTypeException
     * @throws InvalidSelectionException
     * @throws IOException
     * @throws UnknownKeyTypeException
     * @throws GetFileEntryException
     * @throws ConstructPageException
     * @throws AddFileEntryException
     * @throws IteratorException
     * @throws HashEntryNotFoundException
     * @throws InvalidFrameNumberException
     * @throws PageUnpinnedException
     * @throws ReplacerException
     * @throws HFDiskMgrException
     * @throws HFBufMgrException
     * @throws HFException
     * @throws HashOperationException
     * @throws PagePinnedException
     * @throws PageNotFoundException
     * @throws BufMgrException
     * @throws InvalidSlotNumberException
     * @throws KeyTooLongException
     * @throws KeyNotMatchException
     * @throws LeafInsertRecException
     * @throws IndexInsertRecException
     * @throws UnpinPageException
     * @throws PinPageException
     * @throws NodeNotMatchException
     * @throws ConvertException
     * @throws DeleteRecException
     * @throws IndexSearchException
     * @throws LeafDeleteException
     * @throws InsertException
     */
    public boolean batchInsert(String dbFileName, int type, String filepath, int numbf) throws IndexException,
    InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException, InvalidSelectionException,
    IOException, UnknownKeyTypeException, GetFileEntryException, ConstructPageException, AddFileEntryException,
    IteratorException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException,
    ReplacerException, HFDiskMgrException, HFBufMgrException, HFException, HashOperationException,
    PagePinnedException, PageNotFoundException, BufMgrException, InvalidSlotNumberException,
    KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
    UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException,
    IndexSearchException, LeafDeleteException, InsertException {
        updateNumbuf(numbf);
        bigt f = new bigt(dbFileName);
        f.batchInsert(filepath, type, dbFileName, numbf);
        // batchinsert /home/kaushal/DBMSI/Phase3/Dataset/Data1.csv 1 abd 500
        // batchinsert /home/kaushal/DBMSI/Phase3/Dataset/Data2.csv 1 bd 500
        // rowjoin abd bd Echinorhi abc 500
        return true;
    }

}