package BigT;

import global.*;
import index.IndexScan;
import iterator.*;
import iterator.Iterator;

import java.io.*;
import java.util.*;

import diskmgr.*;
import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

/**
 * bigt class : creates bigt files and index files required.
 *
 */

interface Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

}

/**
 * setup the map for use
 * 
 * 
 */
public class bigt implements Filetype, GlobalConst {

    PageId _firstDirPageId; // page number of header page
    int _ftype;
    private boolean _file_deleted;
    private String _fileName;
    private static int tempfilecount = 0;

    public bigt(String name) throws HFDiskMgrException, HFBufMgrException, HFException, IOException {
        _file_deleted = true;
        _fileName = null;

        if (name == null) {
            // If the name is NULL, allocate a temporary name
            // and no logging is required.
            _fileName = "tempHeapFile";
            String useId = new String("user.name");
            String userAccName;
            userAccName = System.getProperty(useId);
            _fileName = _fileName + userAccName;

            String filenum = Integer.toString(tempfilecount);
            _fileName = _fileName + filenum;
            _ftype = TEMP;
            tempfilecount++;

        } else {
            _fileName = name;
            _ftype = ORDINARY;
        }

        // The constructor gets run in two different cases.
        // In the first case, the file is new and the header page
        // must be initialized. This case is detected via a failure
        // in the db->get_file_entry() call. In the second case, the
        // file already exists and all that must be done is to fetch
        // the header page into the buffer pool

        // try to open the file

        Page apage = new Page();
        _firstDirPageId = null;
        if (_ftype == ORDINARY)
            _firstDirPageId = get_file_entry(_fileName);

        if (_firstDirPageId == null) {
            // file doesn't exist. First create it.
            _firstDirPageId = newPage(apage, 1);
            // check error
            if (_firstDirPageId == null)
                throw new HFException(null, "can't new page");

            add_file_entry(_fileName, _firstDirPageId);
            // check error(new exception: Could not add file entry

            BigPage firstDirPage = new BigPage();
            firstDirPage.init(_firstDirPageId, apage);
            PageId pageId = new PageId(INVALID_PAGE);

            firstDirPage.setNextPage(pageId);
            firstDirPage.setPrevPage(pageId);
            unpinPage(_firstDirPageId, true /* dirty */ );

        }
        _file_deleted = false;
        // ASSERTIONS:
        // - ALL private data members of class Heapfile are valid:
        //
        // - _firstDirPageId valid
        // - _fileName valid
        // - no datapage pinned yet
    }

    public MID mapInsert(byte[] recPtr, String filename, int type) throws Exception {
        BTreeFile btf = null;
        if (type != 1) {
            btf = new BTreeFile("btree" + filename + "_" + String.valueOf(type), 0, 100, 0);
        }
        int dpinfoLen = 0;
        int recLen = recPtr.length;
        boolean found;
        MID currentDataPageRid = new MID();
        Page pageinbuffer = new Page();
        BigPage currentDirPage = new BigPage();
        BigPage currentDataPage = new BigPage();

        BigPage nextDirPage = new BigPage();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        PageId nextDirPageId = new PageId(); // OK

        pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);

        found = false;
        Map amap;
        DataPageInfo dpinfo = new DataPageInfo();
        while (found == false) { // Start While01
            // look for suitable dpinfo-struct
            int lm = 0;
            for (currentDataPageRid = currentDirPage
                    .firstMap(); currentDataPageRid != null; currentDataPageRid = currentDirPage
                            .nextMap(currentDataPageRid)) {
                amap = currentDirPage.getMap(currentDataPageRid);
                // System.out.println("AMAP: "+ConvertMap.getIntValue(0, amap.data));
                lm++;
                dpinfo = new DataPageInfo(amap);
                // System.out.println("AMAP: "+amap.getMapOffset());
                // need check the record length == DataPageInfo'slength

                if (recLen <= dpinfo.availspace) {
                    // System.out.println("Ava"+dpinfo.pageId.pid);
                    found = true;
                    break;
                }
            }
            // System.out.println(found);
            // two cases:
            // (1) found == true:
            // currentDirPage has a datapagerecord which can accomodate
            // the record which we have to insert
            // (2) found == false:
            // there is no datapagerecord on the current directory page
            // whose corresponding datapage has enough space free
            // several subcases: see below
            if (found == false) {
                // Start IF01
                // case (2)

                // System.out.println("no datapagerecord on the current directory is OK");
                // System.out.println("dirpage availspace "+currentDirPage.available_space());

                // on the current directory page is no datapagerecord which has
                // enough free space
                //
                // two cases:
                //
                // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
                // if there is enough space on the current directory page
                // to accomodate a new datapagerecord (type DataPageInfo),
                // then insert a new DataPageInfo on the current directory
                // page
                // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
                // look at the next directory page, if necessary, create it.

                if (currentDirPage.available_space() >= DataPageInfo.size) {
                    // Start IF02
                    // case (2.1) : add a new data page record into the
                    // current directory page
                    currentDataPage = _newDatapage(dpinfo);
                    // currentDataPage is pinned! and dpinfo->pageId is also locked
                    // in the exclusive mode

                    // didn't check if currentDataPage==NULL, auto exception

                    // currentDataPage is pinned: insert its record
                    // calling a HFPage function

                    amap = dpinfo.convertToMap();

                    byte[] tmpData = amap.getMapByteArray("di");
                    currentDataPageRid = currentDirPage.insertMap(tmpData);
                    // System.out.println("New: "+Arrays.toString(tmpData));
                    // System.out.println("Dir: "+currentDataPageRid.slotNo + "
                    // "+currentDataPageRid.pageNo.pid);
                    MID tmprid = currentDirPage.firstMap();

                    // need catch error here!
                    if (currentDataPageRid == null)
                        throw new HFException(null, "no space to insert rec.");

                    // end the loop, because a new datapage with its record
                    // in the current directorypage was created and inserted into
                    // the heapfile; the new datapage has enough space for the
                    // record which the user wants to insert

                    found = true;

                } // end of IF02
                else {
                    // Start else 02
                    // case (2.2)
                    nextDirPageId = currentDirPage.getNextPage();
                    // two sub-cases:
                    //
                    // (2.2.1) nextDirPageId != INVALID_PAGE:
                    // get the next directory page from the buffer manager
                    // and do another look
                    // (2.2.2) nextDirPageId == INVALID_PAGE:
                    // append a new directory page at the end of the current
                    // page and then do another loop

                    if (nextDirPageId.pid != INVALID_PAGE) {
                        // Start IF03
                        // case (2.2.1): there is another directory page:
                        unpinPage(currentDirPageId, false);

                        currentDirPageId.pid = nextDirPageId.pid;

                        pinPage(currentDirPageId, currentDirPage, false);

                        // now go back to the beginning of the outer while-loop and
                        // search on the current directory page for a suitable datapage
                    } // End of IF03
                    else { // Start Else03
                           // case (2.2): append a new directory page after currentDirPage
                           // since it is the last directory page
                        nextDirPageId = newPage(pageinbuffer, 1);
                        // need check error!
                        if (nextDirPageId == null)
                            throw new HFException(null, "can't new pae");

                        // initialize new directory page
                        nextDirPage.init(nextDirPageId, pageinbuffer);
                        PageId temppid = new PageId(INVALID_PAGE);
                        nextDirPage.setNextPage(temppid);
                        nextDirPage.setPrevPage(currentDirPageId);

                        // update current directory page and unpin it
                        // currentDirPage is already locked in the Exclusive mode
                        currentDirPage.setNextPage(nextDirPageId);
                        unpinPage(currentDirPageId, true/* dirty */);

                        currentDirPageId.pid = nextDirPageId.pid;
                        currentDirPage = new BigPage(nextDirPage);

                        // remark that MINIBASE_BM->newPage already
                        // pinned the new directory page!
                        // Now back to the beginning of the while-loop, using the
                        // newly created directory page.

                    } // End of else03
                } // End of else02
                  // ASSERTIONS:
                  // - if found == true: search will end and see assertions below
                  // - if found == false: currentDirPage, currentDirPageId
                  // valid and pinned

            } // end IF01
            else { // Start else01
                   // found == true:
                   // we have found a datapage with enough space,
                   // but we have not yet pinned the datapage:

                // ASSERTIONS:
                // - dpinfo valid

                // System.out.println("find the dirpagerecord on current page");

                pinPage(dpinfo.pageId, currentDataPage, false);
                // currentDataPage.openHFpage(pageinbuffer);

            } // End else01
        } // end of While01

        // ASSERTIONS:
        // - currentDirPageId, currentDirPage valid and pinned
        // - dpinfo.pageId, currentDataPageRid valid
        // - currentDataPage is pinned!

        if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
            throw new HFException(null, "invalid PageId");
        // System.out.println(dpinfo.availspace);
        // if (!(currentDataPage.available_space() >= recLen))
        //     throw new SpaceNotAvailableException(null, "no available space");

        if (currentDataPage == null)
            throw new HFException(null, "can't find Data page");

        MID rid;
        int givenstamp = 2147483647, count = 0;
        MID to_deleteMid = null;
        Map akmap = new Map(recPtr, 0);
        akmap.mapSetup();
        String givenRow = akmap.getRowLabel();
        String givenCol = akmap.getColumnLabel();
        String givenVal = akmap.getValue();
        try {

            PageId cuDirPageId = new PageId(_firstDirPageId.pid);
            BigPage cuDirPage = new BigPage();
            BigPage cuDataPage = new BigPage();

            String row, column;
            thisloop: while (cuDirPageId.pid != INVALID_PAGE) {
                pinPage(cuDirPageId, cuDirPage, false);

                rid = new MID();
                Map aMap;
                for (rid = cuDirPage.firstMap(); rid != null; // rid==NULL means no more record
                        rid = cuDirPage.nextMap(rid)) {
                    aMap = cuDirPage.getMap(rid);
                    PageId page = new PageId(ConvertMap.getIntValue(aMap.getMapOffset() + 8, aMap.data));
                    pinPage(page, cuDataPage, false);
                    for (MID mid = cuDataPage.firstMap(); mid != null; mid = cuDataPage.nextMap(mid)) {
                        Map m = cuDataPage.getMap(mid);
                        m.mapSetup();
                        row = m.getRowLabel();
                        column = m.getColumnLabel();
                        if (row.equals(givenRow) && column.equals(givenCol)) {
                            count++;
                            if (m.getTimeStamp() < givenstamp) {
                                to_deleteMid = mid;
                                givenstamp = m.getTimeStamp();
                            }
                            if (count == 3) {
                                unpinPage(page, false);
                                break thisloop;
                            }
                        }
                    }
                    unpinPage(page, false);
                }

                // ASSERTIONS: no more record
                // - we have read all datapage records on
                // the current directory page.

                nextDirPageId = cuDirPage.getNextPage();
                unpinPage(cuDirPageId, false /* undirty */);
                cuDirPageId.pid = nextDirPageId.pid;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        rid = currentDataPage.insertMap(recPtr);
        if (type == 2) {
            String key = givenRow;
            btf.insert(new StringKey(key), rid);
        } else if (type == 3) {
            String key = givenCol;
            btf.insert(new StringKey(key), rid);
        } else if (type == 4) {
            String key = givenCol + givenRow;
            btf.insert(new StringKey(key), rid);
        } else if (type == 5) {
            String key = givenRow + givenVal;
            btf.insert(new StringKey(key), rid);
        }
        if(type!=1){btf.close();}
        dpinfo.recct++;
        dpinfo.availspace = currentDataPage.available_space();
        unpinPage(dpinfo.pageId, true /* = DIRTY */);

        // DataPage is now released
        amap = currentDirPage.returnRecord(currentDataPageRid);

        DataPageInfo dpinfo_ondirpage = new DataPageInfo(amap);

        dpinfo_ondirpage.availspace = dpinfo.availspace;
        dpinfo_ondirpage.recct = dpinfo.recct;
        dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;

        ConvertMap.setIntValue(dpinfo.availspace, amap.getMapOffset(), amap.data);
        ConvertMap.setIntValue(dpinfo.recct, amap.getMapOffset() + 4, amap.data);
        ConvertMap.setIntValue(dpinfo.pageId.pid, amap.getMapOffset() + 8, amap.data);

        unpinPage(currentDirPageId, true /* = DIRTY */);

        if (count == 3) {

            deleteMap(to_deleteMid);
        }
        return rid;

    }

    public MID insertMap(byte[] recPtr) throws Exception {
        int dpinfoLen = 0;
        int recLen = recPtr.length;
        boolean found;
        MID currentDataPageRid = new MID();
        Page pageinbuffer = new Page();
        BigPage currentDirPage = new BigPage();
        BigPage currentDataPage = new BigPage();

        BigPage nextDirPage = new BigPage();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        PageId nextDirPageId = new PageId(); // OK

        pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);

        found = false;
        Map amap;
        DataPageInfo dpinfo = new DataPageInfo();
        while (found == false) { // Start While01
            // look for suitable dpinfo-struct
            for (currentDataPageRid = currentDirPage
                    .firstMap(); currentDataPageRid != null; currentDataPageRid = currentDirPage
                            .nextMap(currentDataPageRid)) {
                amap = currentDirPage.getMap(currentDataPageRid);
                // System.out.println("AMAP: "+ConvertMap.getIntValue(0, amap.data));
                dpinfo = new DataPageInfo(amap);
                // System.out.println("AMAP: "+amap.getMapOffset());
                // need check the record length == DataPageInfo'slength

                if (recLen <= dpinfo.availspace) {
                    // System.out.println("Ava"+dpinfo.pageId.pid);
                    found = true;
                    break;
                }
            }
            // System.out.println(found);
            // two cases:
            // (1) found == true:
            // currentDirPage has a datapagerecord which can accomodate
            // the record which we have to insert
            // (2) found == false:
            // there is no datapagerecord on the current directory page
            // whose corresponding datapage has enough space free
            // several subcases: see below
            if (found == false) {
                // Start IF01
                // case (2)

                // System.out.println("no datapagerecord on the current directory is OK");
                // System.out.println("dirpage availspace "+currentDirPage.available_space());

                // on the current directory page is no datapagerecord which has
                // enough free space
                //
                // two cases:
                //
                // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
                // if there is enough space on the current directory page
                // to accomodate a new datapagerecord (type DataPageInfo),
                // then insert a new DataPageInfo on the current directory
                // page
                // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
                // look at the next directory page, if necessary, create it.

                if (currentDirPage.available_space() >= DataPageInfo.size) {
                    // Start IF02
                    // case (2.1) : add a new data page record into the
                    // current directory page
                    currentDataPage = _newDatapage(dpinfo);
                    // currentDataPage is pinned! and dpinfo->pageId is also locked
                    // in the exclusive mode

                    // didn't check if currentDataPage==NULL, auto exception

                    // currentDataPage is pinned: insert its record
                    // calling a HFPage function

                    amap = dpinfo.convertToMap();

                    byte[] tmpData = amap.getMapByteArray("di");
                    currentDataPageRid = currentDirPage.insertMap(tmpData);
                    // System.out.println("New: "+Arrays.toString(tmpData));
                    // System.out.println("Dir: "+currentDataPageRid.slotNo + "
                    // "+currentDataPageRid.pageNo.pid);
                    MID tmprid = currentDirPage.firstMap();

                    // need catch error here!
                    if (currentDataPageRid == null)
                        throw new HFException(null, "no space to insert rec.");

                    // end the loop, because a new datapage with its record
                    // in the current directorypage was created and inserted into
                    // the heapfile; the new datapage has enough space for the
                    // record which the user wants to insert

                    found = true;

                } // end of IF02
                else {
                    // Start else 02
                    // case (2.2)
                    nextDirPageId = currentDirPage.getNextPage();
                    // two sub-cases:
                    //
                    // (2.2.1) nextDirPageId != INVALID_PAGE:
                    // get the next directory page from the buffer manager
                    // and do another look
                    // (2.2.2) nextDirPageId == INVALID_PAGE:
                    // append a new directory page at the end of the current
                    // page and then do another loop

                    if (nextDirPageId.pid != INVALID_PAGE) {
                        // Start IF03
                        // case (2.2.1): there is another directory page:
                        unpinPage(currentDirPageId, false);

                        currentDirPageId.pid = nextDirPageId.pid;

                        pinPage(currentDirPageId, currentDirPage, false);

                        // now go back to the beginning of the outer while-loop and
                        // search on the current directory page for a suitable datapage
                    } // End of IF03
                    else { // Start Else03
                           // case (2.2): append a new directory page after currentDirPage
                           // since it is the last directory page
                        nextDirPageId = newPage(pageinbuffer, 1);
                        // need check error!
                        if (nextDirPageId == null)
                            throw new HFException(null, "can't new pae");

                        // initialize new directory page
                        nextDirPage.init(nextDirPageId, pageinbuffer);
                        PageId temppid = new PageId(INVALID_PAGE);
                        nextDirPage.setNextPage(temppid);
                        nextDirPage.setPrevPage(currentDirPageId);

                        // update current directory page and unpin it
                        // currentDirPage is already locked in the Exclusive mode
                        currentDirPage.setNextPage(nextDirPageId);
                        unpinPage(currentDirPageId, true/* dirty */);

                        currentDirPageId.pid = nextDirPageId.pid;
                        currentDirPage = new BigPage(nextDirPage);

                        // remark that MINIBASE_BM->newPage already
                        // pinned the new directory page!
                        // Now back to the beginning of the while-loop, using the
                        // newly created directory page.

                    } // End of else03
                } // End of else02
                  // ASSERTIONS:
                  // - if found == true: search will end and see assertions below
                  // - if found == false: currentDirPage, currentDirPageId
                  // valid and pinned

            } // end IF01
            else { // Start else01
                   // found == true:
                   // we have found a datapage with enough space,
                   // but we have not yet pinned the datapage:

                // ASSERTIONS:
                // - dpinfo valid

                // System.out.println("find the dirpagerecord on current page");

                pinPage(dpinfo.pageId, currentDataPage, false);
                // currentDataPage.openHFpage(pageinbuffer);

            } // End else01
        } // end of While01

        // ASSERTIONS:
        // - currentDirPageId, currentDirPage valid and pinned
        // - dpinfo.pageId, currentDataPageRid valid
        // - currentDataPage is pinned!

        if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
            throw new HFException(null, "invalid PageId");

        // if (!(currentDataPage.available_space() >= recLen))
        //     throw new SpaceNotAvailableException(null, "no available space");

        if (currentDataPage == null)
            throw new HFException(null, "can't find Data page");

        MID rid;

        rid = currentDataPage.insertMap(recPtr);
        dpinfo.recct++;
        dpinfo.availspace = currentDataPage.available_space();

        unpinPage(dpinfo.pageId, true /* = DIRTY */);

        amap = currentDirPage.returnRecord(currentDataPageRid);
        ;
        DataPageInfo dpinfo_ondirpage = new DataPageInfo(amap);

        dpinfo_ondirpage.availspace = dpinfo.availspace;
        dpinfo_ondirpage.recct = dpinfo.recct;
        dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;

        ConvertMap.setIntValue(dpinfo.availspace, amap.getMapOffset(), amap.data);
        ConvertMap.setIntValue(dpinfo.recct, amap.getMapOffset() + 4, amap.data);
        ConvertMap.setIntValue(dpinfo.pageId.pid, amap.getMapOffset() + 8, amap.data);

        amap = currentDirPage.returnRecord(currentDataPageRid);

        unpinPage(currentDirPageId, true /* = DIRTY */);

        return rid;
    }

    private BigPage _newDatapage(DataPageInfo dpinfop)
            throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = newPage(apage, 1);

        if (pageId == null)
            throw new HFException(null, "can't new pae");

        // initialize internal values of the new page:

        BigPage hfpage = new BigPage();
        hfpage.init(pageId, apage);

        dpinfop.pageId.pid = pageId.pid;
        dpinfop.recct = 0;
        dpinfop.availspace = hfpage.available_space();

        return hfpage;
    }

    /**
     * Batchinsert2 does the task of inserting whole batch rather than inserting the record one by one. It reduces time by large amount
     * @param filepath: file path of records
     * @param type type for insert
     * @param dbfile bigt file
     * @param numbf number of buffer pages
     * @param btf batchinsert indexing
     * @param btf2 actual btree
     * @return
     * @throws HFBufMgrException
     * @throws InvalidSlotNumberException
     * @throws HFException
     * @throws HFDiskMgrException
     * @throws IOException
     * @throws GetFileEntryException
     * @throws ConstructPageException
     * @throws AddFileEntryException
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
     * @throws IteratorException
     * @throws LeafDeleteException
     * @throws InsertException
     * @throws PageUnpinnedException
     * @throws InvalidFrameNumberException
     * @throws HashEntryNotFoundException
     * @throws ReplacerException
     */
    public boolean batchInsert2(String filepath, int type, String dbfile, int numbf, BTreeFile btf, BTreeFile btf2)
    throws HFBufMgrException, InvalidSlotNumberException, HFException, HFDiskMgrException, IOException,
    GetFileEntryException, ConstructPageException, AddFileEntryException, KeyTooLongException,
    KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException,
    NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
    LeafDeleteException, InsertException, PageUnpinnedException, InvalidFrameNumberException,
    HashEntryNotFoundException, ReplacerException {
        String UTF_BOM = "\uFEFF";
        int dpinfoLen = 0;	
        // int recLen = recPtr.length;
        boolean found;
        FileInputStream fin;
        try{
            fin = new FileInputStream(filepath);
        }catch(FileNotFoundException e){
            throw new FileNotFoundException();
        }
        DataInputStream din = new DataInputStream(fin);
        String line;
        BufferedReader bin = new BufferedReader(new InputStreamReader(din));
        // MID currentDataPageRid = new MID();
        // Page pageinbuffer = new Page();
        Map amap;
        int c = 0;
        BigPage currentDirPage = new BigPage();
        BigPage currentDataPage = new BigPage();
        StringTokenizer st;
        DataPageInfo dpinfo = new DataPageInfo();
        MID currentDataPageRid = new MID();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        ArrayList<Object> ids = getNextDataPageForRecords(currentDirPageId, currentDirPage, currentDataPage, dpinfo);
        currentDirPageId = (PageId) ids.get(0);
        PageId currentDataPageId = (PageId) ids.get(1);
        currentDataPageRid = (MID) ids.get(2);
        pinPage(currentDirPageId, currentDirPage, false);
        pinPage(currentDataPageId, currentDataPage, false);
        while ((line = bin.readLine()) != null) {
            st = new StringTokenizer(line);

            while (st.hasMoreTokens()) {
                String token = st.nextToken();

                StringTokenizer sv = new StringTokenizer(token);
                String rowLabel = sv.nextToken(",");
                String columnLabel = sv.nextToken(",");
                String value = sv.nextToken(",");
                int timeStamp = Integer.parseInt(sv.nextToken(","));
                if(rowLabel.startsWith(UTF_BOM)){
                    rowLabel=rowLabel.substring(1).trim();
                }
                byte[] mapData = new byte[116];

                ConvertMap.setStrValue(rowLabel, 10, mapData);
                ConvertMap.setStrValue(columnLabel, 44, mapData);
                ConvertMap.setIntValue(timeStamp, 78, mapData);
                ConvertMap.setStrValue(value, 82, mapData);

                Map map = new Map(mapData, 0);
                
                map.setHdr(new short[] { 32,32,32 }); 
                byte[] recPtr = map.getMapByteArray();
                MID rid;
                if(dpinfo.availspace > recPtr.length){
                    rid = currentDataPage.insertMap(recPtr);
                    // System.out.println(rid.pageNo + " " + rid.slotNo);
                    dpinfo.recct++;
                    dpinfo.availspace = currentDataPage.available_space();
                }else{
                    unpinPage(currentDataPageId, true);
                    amap = currentDirPage.returnRecord(currentDataPageRid);
                    byte[] amap_data = amap.getMapByteArray("di");
                    DataPageInfo dpinfo_ondirpage = new DataPageInfo(amap_data);
                    
                    dpinfo_ondirpage.availspace = dpinfo.availspace;
                    dpinfo_ondirpage.recct = dpinfo.recct;
                    dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;

                    ConvertMap.setIntValue(dpinfo.availspace, amap.getMapOffset(), amap.data);
                    ConvertMap.setIntValue(dpinfo.recct, amap.getMapOffset()+4, amap.data);
                    ConvertMap.setIntValue(dpinfo.pageId.pid, amap.getMapOffset()+8, amap.data);
                    unpinPage(currentDirPageId, true);
                    ids = getNextDataPageForRecords(currentDirPageId, currentDirPage, currentDataPage, dpinfo);
                    currentDirPageId = (PageId) ids.get(0);
                    currentDataPageId = (PageId) ids.get(1);
                    currentDataPageRid = (MID) ids.get(2);
                    pinPage(currentDirPageId, currentDirPage, false);
                    pinPage(currentDataPageId, currentDataPage, false);
                    rid = currentDataPage.insertMap(recPtr);
                    dpinfo.pageId = currentDataPageId;
                    dpinfo.recct = 1;
                    dpinfo.availspace = currentDataPage.available_space();
                }
                if(type==2){
                    btf2.insert(new StringKey(rowLabel), rid);
                }else if(type==3){
                    btf2.insert(new StringKey(columnLabel), rid);
                }else if(type==4){
                    btf2.insert(new StringKey(columnLabel+rowLabel), rid);
                }else if(type==5){
                    btf2.insert(new StringKey(rowLabel+value), rid);
                }
                if (type == 1){btf.insert(new StringKey(columnLabel+rowLabel), rid);}
                c++;
                System.out.println("Inserted "+ c +"th record");
            }
        }
        
        unpinPage(currentDataPageId, true);
        amap = currentDirPage.returnRecord(currentDataPageRid);
        DataPageInfo dpinfo_ondirpage = new DataPageInfo(amap);
        
        dpinfo_ondirpage.availspace = dpinfo.availspace;
        dpinfo_ondirpage.recct = dpinfo.recct;
        dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;

        ConvertMap.setIntValue(dpinfo.availspace, amap.getMapOffset(), amap.data);
        ConvertMap.setIntValue(dpinfo.recct, amap.getMapOffset()+4, amap.data);
        ConvertMap.setIntValue(dpinfo.pageId.pid, amap.getMapOffset()+8, amap.data);
        unpinPage(currentDirPageId, true);
        bin.close();
        return true;
        
    }

    public ArrayList<Object> getNextDataPageForRecords(PageId currentDirPageId, BigPage currentDirPage, BigPage currentDataPage, DataPageInfo dpinfo) throws HFBufMgrException,
    InvalidSlotNumberException, IOException, HFException, HFDiskMgrException {
        pinPage(currentDirPageId, currentDirPage, false);
        ArrayList<Object> arrayList = new ArrayList<>();
        boolean found = false;
        Map amap;
        PageId nextDirPageId;
        Page pageinbuffer = new Page();
        BigPage nextDirPage = new BigPage();
        MID currentDataPageRid = new MID();
        boolean isDirectory_changed = false;
        while (found == false) { //Start While01
            // look for suitable dpinfo-struct
            for (currentDataPageRid = currentDirPage.firstMap();
                currentDataPageRid != null;
                currentDataPageRid = 
                currentDirPage.nextMap(currentDataPageRid)) {
                    
                amap = currentDirPage.getMap(currentDataPageRid);
                dpinfo = new DataPageInfo(amap);
                // System.out.println("AMAP: "+amap.getMapOffset());
                // need check the record length == DataPageInfo'slength
                
                if(116 <= dpinfo.availspace)
                {
                    // System.out.println("Ava"+dpinfo.pageId.pid);
                    found = true;
                    break;
                }  
            }
            if(found == false) { 
                //Start IF01
                // case (2)
                
                //System.out.println("no datapagerecord on the current directory is OK");
                //System.out.println("dirpage availspace "+currentDirPage.available_space());
                
                // on the current directory page is no datapagerecord which has
                // enough free space
                //
                // two cases:
                //
                // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
                //         if there is enough space on the current directory page
                //         to accomodate a new datapagerecord (type DataPageInfo),
                //         then insert a new DataPageInfo on the current directory
                //         page
                // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
                //         look at the next directory page, if necessary, create it.
            
                if(currentDirPage.available_space() >= DataPageInfo.size) {
                    //Start IF02
                    // case (2.1) : add a new data page record into the
                    //              current directory page
                    currentDataPage = _newDatapage(dpinfo); 
                    // currentDataPage is pinned! and dpinfo->pageId is also locked
                    // in the exclusive mode  
                    
                    // didn't check if currentDataPage==NULL, auto exception
                    
                    
                    // currentDataPage is pinned: insert its record
                    // calling a HFPage function
                    
                    
                    
                    amap = dpinfo.convertToMap();
                    
                    byte [] tmpData = amap.getMapByteArray("di");
                    currentDataPageRid = currentDirPage.insertMap(tmpData);
                    // System.out.println("New: "+Arrays.toString(tmpData));
                    // System.out.println("Dir: "+currentDataPageRid.slotNo + " "+currentDataPageRid.pageNo.pid);
                    MID tmprid = currentDirPage.firstMap();
                    
            
                    // need catch error here!
                    if(currentDataPageRid == null)
                        throw new HFException(null, "no space to insert rec.");  
                    unpinPage(dpinfo.pageId, false);
                    // end the loop, because a new datapage with its record
                    // in the current directorypage was created and inserted into
                    // the heapfile; the new datapage has enough space for the
                    // record which the user wants to insert
            
                    found = true;
            
                } //end of IF02
                else {  
                    //Start else 02
                    // case (2.2)
                    nextDirPageId = currentDirPage.getNextPage();
                    // two sub-cases:
                    //
                    // (2.2.1) nextDirPageId != INVALID_PAGE:
                    //         get the next directory page from the buffer manager
                    //         and do another look
                    // (2.2.2) nextDirPageId == INVALID_PAGE:
                    //         append a new directory page at the end of the current
                    //         page and then do another loop
                        
                    if (nextDirPageId.pid != INVALID_PAGE) {
                        //Start IF03
                        // case (2.2.1): there is another directory page:
                        unpinPage(currentDirPageId, false);
                        
                        currentDirPageId.pid = nextDirPageId.pid;
                        
                        pinPage(currentDirPageId, currentDirPage, false);
                        
                        // now go back to the beginning of the outer while-loop and
                        // search on the current directory page for a suitable datapage
                    } //End of IF03
                    else
                    {  //Start Else03
                        // case (2.2): append a new directory page after currentDirPage
                        //             since it is the last directory page
                        isDirectory_changed = true;
                        nextDirPageId = newPage(pageinbuffer, 1);
                        // need check error!
                        if(nextDirPageId == null)
                            throw new HFException(null, "can't new pae");
                
                        // initialize new directory page
                        nextDirPage.init(nextDirPageId, pageinbuffer);
                        PageId temppid = new PageId(INVALID_PAGE);
                        nextDirPage.setNextPage(temppid);
                        nextDirPage.setPrevPage(currentDirPageId);
                        
                        // update current directory page and unpin it
                        // currentDirPage is already locked in the Exclusive mode
                        currentDirPage.setNextPage(nextDirPageId);
                        unpinPage(currentDirPageId, true/*dirty*/);
                        
                        currentDirPageId.pid = nextDirPageId.pid;
                        currentDirPage = new BigPage(nextDirPage);
                
                        // remark that MINIBASE_BM->newPage already
                        // pinned the new directory page!
                        // Now back to the beginning of the while-loop, using the
                        // newly created directory page.
                
                    } //End of else03
                } // End of else02
                // ASSERTIONS:
                // - if found == true: search will end and see assertions below
                // - if found == false: currentDirPage, currentDirPageId
                //   valid and pinned
            
            }//end IF01
            else
            { //Start else01
                // found == true:
                // we have found a datapage with enough space,
                // but we have not yet pinned the datapage:
                
                // ASSERTIONS:
                // - dpinfo valid
                
                // System.out.println("find the dirpagerecord on current page");
                
                // pinPage(dpinfo.pageId, currentDataPage, false);
                //currentDataPage.openHFpage(pageinbuffer);
            
            
            }
        }
        unpinPage(currentDirPageId, false);
        arrayList.add(currentDirPageId);
        arrayList.add(dpinfo.pageId);
        arrayList.add(currentDataPageRid);
        arrayList.add(isDirectory_changed);
        return arrayList;
    }

    public boolean batchInsert(String filepath, int type, String dbfile, int numbf){
        String UTF_BOM = "\uFEFF";
        ArrayList<MID> toDelete = new ArrayList<>();
        PriorityQueue<MID> list = new PriorityQueue<>(10, new MIDComparator());
        try{
            FileInputStream fin;
            fin = new FileInputStream(filepath);
            DataInputStream din = new DataInputStream(fin);
            BufferedReader bin = new BufferedReader(new InputStreamReader(din));
            FldSpec[] proj_list = new FldSpec[4];
            RelSpec rel = new RelSpec(RelSpec.outer);
            proj_list[0]= new FldSpec(rel, 1);
            proj_list[1]= new FldSpec(rel, 2);
            proj_list[2]= new FldSpec(rel, 3);
            proj_list[3]= new FldSpec(rel, 4);
            BTreeFile btf = new BTreeFile(dbfile+"_insert", 0, 100, 0);
            BTreeFile btf2 = new BTreeFile(dbfile+"_insert_2", 0, 100, 0);
            bigt samplebigt = new bigt("tempBigt");
            BTreeFile btf_actual = null;
            if(type != 1){
                btf_actual = new BTreeFile("btree"+dbfile+"_"+String.valueOf(type), 0, 64, 0);
            }
            FileScan fs1 = new FileScan(dbfile, type, new short[]{32,32,32}, 4, proj_list, null);
            while (true) {
                MapMID m = fs1.get_next_mapMID();
                if(m == null){break;}
                btf.insert(new StringKey(m.getMap().getColumnLabel()+m.getMap().getRowLabel()), m.getMID());
            }
            fs1.close();
            String line;

            int count = 0;
            StringTokenizer st;
            System.out.println("Batch Inserting records! Wait for few minutes!");
            if(type == 1){
                batchInsert2(filepath, type, dbfile, numbf, btf, btf2);
            }else {
                samplebigt.batchInsert2(filepath, type, dbfile, numbf, btf, btf2);
            }
            AttrType[] attrType = new AttrType[4];
            attrType[0] = new AttrType(AttrType.attrString);
            attrType[1] = new AttrType(AttrType.attrString);
            attrType[2] = new AttrType(AttrType.attrInteger);
            attrType[3] = new AttrType(AttrType.attrString);
            System.out.println("Storing records according to type " + type + " storage fashion.");
            if(type == 2){
                int order = 7;
                // FileScan fScan = new FileScan("tempBigt", type, new short[]{32,32,32}, 4, proj_list, null);
                // Sort s = new Sort(new short[]{32,32,32}, fScan, order, new MapOrder(MapOrder.Ascending), 32, numbf, order);
                IndexScan indexScan = new IndexScan(new IndexType(IndexType.Row_Label_Index), "tempBigt", dbfile+"_insert_2", new short[]{32,32,32}, 4, 4, proj_list, null, 1, false, null);
                boolean done2 = false;
                while(!done2){
                    Map m = indexScan.get_next();
                    if(m == null)
                        break;
                    m.print();
                    MID mid = insertMap(m.getMapByteArray());
                    String key1 = m.getRowLabel();
                    String key2 = m.getColumnLabel();
                    String key = key2 + key1;
                    btf.insert(new StringKey(key), mid);
                    btf_actual.insert(new StringKey(m.getRowLabel()), mid);
                }
                indexScan.close();
                // s.close();
                // fScan.close();
            }else if(type == 3){
                int order = 8;
                // FileScan fScan = new FileScan("tempBigt", type, new short[]{32,32,32}, 4, proj_list, null);
                // Sort s = new Sort(new short[]{32,32,32}, fScan, order, new MapOrder(MapOrder.Ascending), 32, numbf, order);
                IndexScan indexScan = new IndexScan(new IndexType(IndexType.Column_Label_Index), "tempBigt", dbfile+"_insert_2", new short[]{32,32,32}, 4, 4, proj_list, null, 1, false, null);
                boolean done2 = false;
                while(!done2){
                    Map m = indexScan.get_next();
                    if(m == null)
                        break;
                    MID mid = insertMap(m.getMapByteArray());
                    String key1 = m.getRowLabel();
                    String key2 = m.getColumnLabel();
                    String key = key2 + key1;
                    btf.insert(new StringKey(key), mid);
                    btf_actual.insert(new StringKey(m.getColumnLabel()), mid);
                }
                indexScan.close();
                // s.close();
                // fScan.close();
            }else if(type == 4){
                int order = 2;
                // FileScan fScan = new FileScan("tempBigt", type, new short[]{32,32,32}, 4, proj_list, null);
                // Sort s = new Sort(new short[]{32,32,32}, fScan, order, new MapOrder(MapOrder.Ascending), 32, numbf, order);
                IndexScan indexScan = new IndexScan(new IndexType(IndexType.Column_Row_Label_Index), "tempBigt", dbfile+"_insert_2", new short[]{32,32,32}, 4, 4, proj_list, null, 1, false, null);
                boolean done2 = false;
                while(!done2){
                    Map m = indexScan.get_next();
                    if(m == null)
                        break;
                    MID mid = insertMap(m.getMapByteArray());
                    String key1 = m.getRowLabel();
                    String key2 = m.getColumnLabel();
                    String key = key2 + key1;
                    btf.insert(new StringKey(key), mid);
                    btf_actual.insert(new StringKey(m.getColumnLabel() + m.getRowLabel()), mid);
                }
                indexScan.close();
                // s.close();
                // fScan.close();
            }else if(type == 5){
                int order = 6;
                // FileScan fScan = new FileScan("tempBigt", type, new short[]{32,32,32}, 4, proj_list, null);
                // Sort s = new Sort(new short[]{32,32,32}, fScan, order, new MapOrder(MapOrder.Ascending), 32, numbf, order);
                IndexScan indexScan = new IndexScan(new IndexType(IndexType.Row_Label_Value_Index), "tempBigt", dbfile+"_insert_2", new short[]{32,32,32}, 4, 4, proj_list, null, 1, false, null);
                boolean done2 = false;
                while(!done2){
                    Map m = indexScan.get_next();
                    if(m == null)
                        break;
                    MID mid = insertMap(m.getMapByteArray());
                    String key1 = m.getRowLabel();
                    String key2 = m.getColumnLabel();
                    String key = key2 + key1;
                    btf.insert(new StringKey(key), mid);
                    btf_actual.insert(new StringKey(m.getRowLabel() + m.getValue()), mid);
                }
                indexScan.close();
                // s.close();
                // fScan.close();
            }
            btf2.destroyFile();
            // BT.printAllLeafPages(btf.getHeaderPage());
            System.out.println("Sample insertion finished. Checking for duplicate entries");
            ArrayList<String> keyDone = new ArrayList<>();
            int c2 = 0;
            PriorityQueue<MapMID> pq = new PriorityQueue<MapMID>(5, new MapComparator());
            FileScan fs = new FileScan(dbfile, type, new short[]{32,32,32}, 4, proj_list, null);
            boolean done = false;
            while (!done) {
                Map map = fs.get_next();
                if(map == null){
                    break;
                }
                // map.print();
                map.mapSetup();
                
                if(keyDone.contains(map.getColumnLabel() + map.getRowLabel())){
                    continue;
                }else{
                    keyDone.add(map.getColumnLabel() + map.getRowLabel());
                }
                // System.out.println("Deleting entry for "+map.getRowLabel()+" "+map.getColumnLabel());
                CondExpr[] ex = new CondExpr[2];
                ex[0] = new CondExpr();
                ex[0].fldNo = 1;
                ex[0].type1 = new AttrType(AttrType.attrSymbol);
                ex[0].op = new AttrOperator(AttrOperator.aopEQ);
                ex[0].type2 = new AttrType(AttrType.attrString);
                ex[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                ex[0].operand2.string = map.getColumnLabel() + map.getRowLabel();
                IndexScan iScan = new IndexScan(new IndexType(IndexType.Column_Row_Label_Index), dbfile, dbfile+"_insert", new short[]{32,32,32}, 4, 4, proj_list, null, 2, false, ex);
                boolean done2 = false;
                
                while(!done2){
                    MapMID map2 = iScan.get_next_MapMid();
                    if(map2 == null){
                        break;
                    }
                    pq.add(map2);
                }
                int c3 = 0;
                if(pq.size() > 3){
                    while (!pq.isEmpty()){
                        MapMID mm  = pq.poll();
                        if(c3 > 2){
                            // btf.Delete(new StringKey(mm.getMap().getColumnLabel()+mm.getMap().getRowLabel()), mm.getMID());
                            list.add(mm.getMID());
                        }
                        c3++;
                    }
                }else {
                    pq.clear();
                }
                c2++;
                System.out.println("Checked the versions for "+c2+"th entry");
            }
            System.out.println("Deleting duplicate records "+ list.size());
            batchDelete(list);
            System.out.println("Batchinsertion finished! transaction info: \nTotal Map count: "+getMapCnt()+ "\nTotal Distinct Row Count: "+ getRowCnt().size() +"\nTotal Distinct Column Count: "+getColumnCnt().size());
            
            btf.destroyFile();
            samplebigt.deleteBigt();
            if(type != 1){btf_actual.close();}
            bin.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private boolean  _findDataPage( MID rid,
        PageId dirPageId, BigPage dirpage,
        PageId dataPageId, BigPage datapage,
        MID rpDataPageRid) 
    throws InvalidSlotNumberException, 
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   Exception
    {
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        
        BigPage currentDirPage = new BigPage();
        BigPage currentDataPage = new BigPage();
        MID currentDataPageRid = new MID();
        PageId nextDirPageId = new PageId();
        // datapageId is stored in dpinfo.pageId 
        
        
        pinPage(currentDirPageId, currentDirPage, false/*read disk*/);
        
        Map amap = new Map();
        
        while (currentDirPageId.pid != INVALID_PAGE) {// Start While01
	  // ASSERTIONS:
	  //  currentDirPage, currentDirPageId valid and pinned and Locked.
	  
            for( currentDataPageRid = currentDirPage.firstMap();
                currentDataPageRid != null;
                currentDataPageRid = currentDirPage.nextMap(currentDataPageRid)) {
                try{
                    amap = currentDirPage.getMap(currentDataPageRid);
                } catch (InvalidSlotNumberException e)// check error! return false(done) 
                {
                    return false;
                }
                
                DataPageInfo dpinfo = new DataPageInfo(amap);
                try{
                    pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);
            
                    //check error;need unpin currentDirPage
                } catch (Exception e) {
                    unpinPage(currentDirPageId, false/*undirty*/);
                    dirpage = null;
                    datapage = null;
                    throw e;
                }
	      
	      
	      
            // ASSERTIONS:
            // - currentDataPage, currentDataPageRid, dpinfo valid
            // - currentDataPage pinned
	      
                if(dpinfo.pageId.pid==rid.pageNo.pid) {
                    amap = currentDataPage.returnRecord(rid);
                    // found user's record on the current datapage which itself
                    // is indexed on the current dirpage.  Return both of these.
                    
                    dirpage.setpage(currentDirPage.getpage());
                    dirPageId.pid = currentDirPageId.pid;
                    
                    datapage.setpage(currentDataPage.getpage());
                    dataPageId.pid = dpinfo.pageId.pid;
                    
                    rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
                    rpDataPageRid.slotNo = currentDataPageRid.slotNo;
                    return true;
                } else {
                    // user record not found on this datapage; unpin it
                    // and try the next one
                    unpinPage(dpinfo.pageId, false /*undirty*/);
                
                }
                
            }
	  
            // if we would have found the correct datapage on the current
            // directory page we would have already returned.
            // therefore:
            // read in next directory page:
        
            nextDirPageId = currentDirPage.getNextPage();
            try{
                unpinPage(currentDirPageId, false /*undirty*/);
            }
            catch(Exception e) {
                throw new HFException (e, "heapfile,_find,unpinpage failed");
            }
	  
            currentDirPageId.pid = nextDirPageId.pid;
            if(currentDirPageId.pid != INVALID_PAGE) {
                pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
                if(currentDirPage == null)
                    throw new HFException(null, "pinPage return null page");  
            }
	  
	  
	    } // end of While01
         // checked all dir pages and all data pages; user record not found:(
      
        dirPageId.pid = dataPageId.pid = INVALID_PAGE;
        
        return false; 
      
      
    } // end of _findDatapage		     
      
    public void deleteBigt() throws InvalidSlotNumberException, IOException, FileAlreadyDeletedException,
            HFBufMgrException, HFDiskMgrException {
        if(_file_deleted ) 
   	        throw new FileAlreadyDeletedException(null, "file alread deleted");
      
      
        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;
        
        // Deallocate all data pages
        PageId currentDirPageId = new PageId();
        currentDirPageId.pid = _firstDirPageId.pid;
        PageId nextDirPageId = new PageId();
        nextDirPageId.pid = 0;
        Page pageinbuffer = new Page();
        BigPage currentDirPage =  new BigPage();
        Map aMap;
        
        pinPage(currentDirPageId, currentDirPage, false);
        //currentDirPage.openHFpage(pageinbuffer);
        
        MID rid = new MID();
        while(currentDirPageId.pid != INVALID_PAGE)
        {      
        for(rid = currentDirPage.firstMap();
            rid != null;
            rid = currentDirPage.nextMap(rid))
            {
            aMap = currentDirPage.getMap(rid);
            DataPageInfo dpinfo = new DataPageInfo( aMap);
            //int dpinfoLen = arecord.length;
            
            freePage(dpinfo.pageId);
            
            }
        // ASSERTIONS:
        // - we have freePage()'d all data pages referenced by
        // the current directory page.
        
        nextDirPageId = currentDirPage.getNextPage();
        freePage(currentDirPageId);
        
        currentDirPageId.pid = nextDirPageId.pid;
        if (nextDirPageId.pid != INVALID_PAGE) 
            {
            pinPage(currentDirPageId, currentDirPage, false);
            //currentDirPage.openHFpage(pageinbuffer);
            }
        }
        
        delete_file_entry( _fileName );
    }

    public int getMapCnt() throws HFBufMgrException, InvalidSlotNumberException, IOException {
        int answer = 0;
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        
        PageId nextDirPageId = new PageId(0);
        
        BigPage currentDirPage = new BigPage();
        Page pageinbuffer = new Page();
        while(currentDirPageId.pid != INVALID_PAGE)
        {
            pinPage(currentDirPageId, currentDirPage, false);
        
            MID rid = new MID();
            Map aMap;
            for (rid = currentDirPage.firstMap();
                rid != null;	// rid==NULL means no more record
                rid = currentDirPage.nextMap(rid))
            {
                aMap = currentDirPage.getMap(rid);
                DataPageInfo dpinfo = new DataPageInfo(aMap);
                
                answer += dpinfo.recct;
            }
        
                // ASSERTIONS: no more record
                // - we have read all datapage records on
                //   the current directory page.
        
            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId, false /*undirty*/);
            currentDirPageId.pid = nextDirPageId.pid;
        }
        
        // ASSERTIONS:
        // - if error, exceptions
        // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
        // - if not yet end of heapfile: currentDirPageId valid
        
        
        return answer;
    }

    public ArrayList<String> getRowCnt() throws HFBufMgrException, InvalidSlotNumberException, IOException {
        PageId cuDirPageId = new PageId(_firstDirPageId.pid);
        BigPage cuDirPage = new BigPage();
        BigPage cuDataPage = new BigPage();
        ArrayList<String> rows = new ArrayList<>();
        PageId nextDirPageId = new PageId();
        String row;
        while(cuDirPageId.pid != INVALID_PAGE)
        {
            pinPage(cuDirPageId, cuDirPage, false);
        
            MID rid = new MID();
            Map aMap;
            for (rid = cuDirPage.firstMap();
                rid != null;	// rid==NULL means no more record
                rid = cuDirPage.nextMap(rid))
            {
                aMap = cuDirPage.getMap(rid);
                // DataPageInfo dInfo = new DataPageInfo(aMap);
                PageId page = new PageId(ConvertMap.getIntValue(aMap.getMapOffset()+8, aMap.data));
                pinPage(page, cuDataPage, false);
                // pinPage(dInfo.pageId, cuDataPage, false);
                for(MID mid = cuDataPage.firstMap(); mid != null; mid = cuDataPage.nextMap(mid)){
                    Map m = cuDataPage.getMap(mid);
                    // System.out.println("here"); 
                    m.mapSetup();
                    row = m.getRowLabel();
                    if (!rows.contains(row)) {
                        rows.add(row);
                    }
                }
                unpinPage(page, false);
            }
        
                // ASSERTIONS: no more record
                // - we have read all datapage records on
                //   the current directory page.
        
            nextDirPageId = cuDirPage.getNextPage();
            unpinPage(cuDirPageId, false /*undirty*/);
            cuDirPageId.pid = nextDirPageId.pid;
        }
        
        // ASSERTIONS:
        // - if error, exceptions
        // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
        // - if not yet end of heapfile: currentDirPageId valid
        
        
        return rows;
    }

    public ArrayList<String> getColumnCnt() throws HFBufMgrException, InvalidSlotNumberException, IOException {

        PageId cuDirPageId = new PageId(_firstDirPageId.pid);
        BigPage cuDirPage = new BigPage();
        BigPage cuDataPage = new BigPage();
        ArrayList<String> columnList = new ArrayList<>();
        PageId nextDirPageId = new PageId();
        String row,column;
        thisloop: while(cuDirPageId.pid != INVALID_PAGE)
        {
            pinPage(cuDirPageId, cuDirPage, false);
        
            MID rid = new MID();
            Map aMap;
            for (rid = cuDirPage.firstMap();
                rid != null;	// rid==NULL means no more record
                rid = cuDirPage.nextMap(rid))
            {
                aMap = cuDirPage.getMap(rid);
                // DataPageInfo dInfo = new DataPageInfo(aMap);
                PageId page = new PageId(ConvertMap.getIntValue(aMap.getMapOffset()+8, aMap.data));
                pinPage(page, cuDataPage, false);
                // pinPage(dInfo.pageId, cuDataPage, false);
                for(MID mid = cuDataPage.firstMap(); mid != null; mid = cuDataPage.nextMap(mid)){
                    Map m = cuDataPage.getMap(mid);
                    // System.out.println("here"); 
                    m.mapSetup();
                    row = m.getRowLabel();
                    column = m.getColumnLabel();
                    if (!columnList.contains(column)) {
                        columnList.add(column);
                    }
                }
                unpinPage(page, false);
            }
        
                // ASSERTIONS: no more record
                // - we have read all datapage records on
                //   the current directory page.
        
            nextDirPageId = cuDirPage.getNextPage();
            unpinPage(cuDirPageId, false /*undirty*/);
            cuDirPageId.pid = nextDirPageId.pid;
        }
        
        // ASSERTIONS:
        // - if error, exceptions
        // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
        // - if not yet end of heapfile: currentDirPageId valid
        
        
        return columnList;
    }

    public Map getMap(MID mid)
            throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {
        boolean status;
        BigPage dirPage = new BigPage();
        PageId currentDirPageId = new PageId();
        BigPage dataPage = new BigPage();
        PageId currentDataPageId = new PageId();
        MID currentDataPageRid = new MID();

        status = _findDataPage(mid,
			     currentDirPageId, dirPage, 
			     currentDataPageId, dataPage,
			     currentDataPageRid);
      
        if(status != true) return null; 

        Map amap = new Map();
        amap = dataPage.getMap(mid);
        unpinPage(currentDataPageId,false /*undirty*/);
      
        unpinPage(currentDirPageId,false /*undirty*/);
      
      
        return  amap;
    }

    public boolean deleteMap(MID mid)
            throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {
        boolean status;
        BigPage currentDirPage = new BigPage();
        PageId currentDirPageId = new PageId();
        BigPage currentDataPage = new BigPage();
        PageId currentDataPageId = new PageId();
        MID currentDataPageRid = new MID();
        
        status = _findDataPage(mid,
                    currentDirPageId, currentDirPage, 
                    currentDataPageId, currentDataPage,
                    currentDataPageRid);
      
        if(status != true) return status;	// record not found
      
        // ASSERTIONS:
        // - currentDirPage, currentDirPageId valid and pinned
        // - currentDataPage, currentDataPageid valid and pinned
        
        // get datapageinfo from the current directory page:
        Map amap;	
        
        amap = currentDirPage.returnRecord(currentDataPageRid);
        DataPageInfo pdpinfo = new DataPageInfo(amap);
        // System.out.println("Hey: "+Arrays.toString(amap.data));
        
        // delete the record on the datapage
        currentDataPage.deleteMap(mid);
        byte [] recbyt = new byte[4];
        byte [] available_space_by = new byte[4];
        System.arraycopy(amap.data, amap.getMapOffset(), available_space_by, 0, 4);
        System.arraycopy(amap.data, amap.getMapOffset()+4, recbyt, 0, 4);
        int recm = ConvertMap.getIntValue(0, recbyt);
        int as = ConvertMap.getIntValue(0, available_space_by);
        // System.out.println("Reccount: "+recm);
        recm--;
        ConvertMap.setIntValue(recm, amap.getMapOffset()+4, amap.data);
        amap = currentDirPage.returnRecord(currentDataPageRid);
        // System.out.println(Arrays.toString(amap.data));
        // pdpinfo.flushToMap();	//Write to the buffer pool
        
        if (recm >= 1) {
            // more records remain on datapage so it still hangs around.  
            // we just need to modify its directory entry
            
            pdpinfo.availspace = currentDataPage.available_space();
            // pdpinfo.flushToMap();
            ConvertMap.setIntValue(as, amap.getMapOffset(), amap.data);
            
            unpinPage(currentDataPageId, true /* = DIRTY*/);
            
            unpinPage(currentDirPageId, true /* = DIRTY */);
	  
        } else {
            // the record is already deleted:
            // we're removing the last record on datapage so free datapage
            // also, free the directory page if 
            //   a) it's not the first directory page, and 
            //   b) we've removed the last DataPageInfo record on it.
            
            // delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
            unpinPage(currentDataPageId, false /*undirty*/);
            
            freePage(currentDataPageId);
            
            // delete corresponding DataPageInfo-entry on the directory page:
            // currentDataPageRid points to datapage (from for loop above)
            
            currentDirPage.deleteMap(currentDataPageRid);
            
            
            // ASSERTIONS:
            // - currentDataPage, currentDataPageId invalid
            // - empty datapage unpinned and deleted
            
            // now check whether the directory page is empty:
            
            currentDataPageRid = currentDirPage.firstMap();
        
            // st == OK: we still found a datapageinfo record on this directory page
            PageId pageId;
            pageId = currentDirPage.getPrevPage();
            if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE)) {
                // the directory-page is not the first directory page and it is empty:
                // delete it
                
                // point previous page around deleted page:
                
                BigPage prevDirPage = new BigPage();
                pinPage(pageId, prevDirPage, false);

                pageId = currentDirPage.getNextPage();
                prevDirPage.setNextPage(pageId);
                pageId = currentDirPage.getPrevPage();
                unpinPage(pageId, true /* = DIRTY */);
                
                
                // set prevPage-pointer of next Page
                pageId = currentDirPage.getNextPage();
	            if(pageId.pid != INVALID_PAGE) {
                    BigPage nextDirPage = new BigPage();
                    pageId = currentDirPage.getNextPage();
                    pinPage(pageId, nextDirPage, false);
                    
                    //nextDirPage.openHFpage(apage);
                    
                    pageId = currentDirPage.getPrevPage();
                    nextDirPage.setPrevPage(pageId);
                    pageId = currentDirPage.getNextPage();
                    unpinPage(pageId, true /* = DIRTY */);
                }
	      
                // delete empty directory page: (automatically unpinned?)
                unpinPage(currentDirPageId, false/*undirty*/);
                freePage(currentDirPageId);
	      
	        } else {
                // either (the directory page has at least one more datapagerecord
                // entry) or (it is the first directory page):
                // in both cases we do not delete it, but we have to unpin it:
                
                unpinPage(currentDirPageId, true /* == DIRTY */);
	        }
        }
        
        return true;
    }

    /**
     * Batchdelete deletes an entire list of Maps in one go, rather than deleteMap which deletes map one by one, everytime traversing an entire bigt
     * @param list list of MID to delete
     * @return
     * @throws HFBufMgrException
     * @throws IOException
     * @throws InvalidSlotNumberException
     * @throws HFException
     */
    public boolean batchDelete(PriorityQueue<MID> list) throws HFBufMgrException, IOException,
    InvalidSlotNumberException, HFException {
        BigPage currentDirPage = new BigPage();
        BigPage currentDataPage = new BigPage();
        PageId currentDataPageId = new PageId();
        PageId nextDirPageId = new PageId();
        MID currentDataPageRid = new MID();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        pinPage(currentDirPageId, currentDirPage, false);
        MID thisPageRid = currentDirPage.firstMap();
        DataPageInfo dpinfo = null;
        Map amap = new Map();
        listLoop: while(!list.isEmpty()){
            MID toDelete = list.poll();
            if(toDelete.pageNo.pid == currentDataPageId.pid){
                currentDataPage.deleteMap(toDelete);
                byte [] recbyt = new byte[4];
                byte [] available_space_by = new byte[4];
                System.arraycopy(amap.data, amap.getMapOffset(), available_space_by, 0, 4);
                System.arraycopy(amap.data, amap.getMapOffset()+4, recbyt, 0, 4);
                int recm = ConvertMap.getIntValue(0, recbyt);
                int as = ConvertMap.getIntValue(0, available_space_by);
                recm--;
                if (recm >= 1) {
                    as = currentDataPage.available_space();
                    ConvertMap.setIntValue(as, amap.getMapOffset(), amap.data);
                    ConvertMap.setIntValue(recm, amap.getMapOffset()+4, amap.data);
                    continue listLoop;
                }else {
                    unpinPage(currentDataPageId, false /*undirty*/);
                    freePage(currentDataPageId);

                    currentDirPage.deleteMap(currentDataPageRid);
                    currentDataPageRid = currentDirPage.firstMap();
                    PageId pageId;
                    pageId = currentDirPage.getPrevPage();
                    if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE)) {
                        
                        BigPage prevDirPage = new BigPage();
                        pinPage(pageId, prevDirPage, false);
        
                        pageId = currentDirPage.getNextPage();
                        prevDirPage.setNextPage(pageId);
                        pageId = currentDirPage.getPrevPage();
                        
                        unpinPage(pageId, false);
                        // set prevPage-pointer of next Page
                        pageId = currentDirPage.getNextPage();
                        if(pageId.pid != INVALID_PAGE) {
                            BigPage nextDirPage = new BigPage();
                            pageId = currentDirPage.getNextPage();
                            pinPage(pageId, nextDirPage, false);
                            
                            //nextDirPage.openHFpage(apage);
                            
                            pageId = currentDirPage.getPrevPage();
                            nextDirPage.setPrevPage(pageId);
                            pageId = currentDirPage.getNextPage();
                            unpinPage(pageId, true /* = DIRTY */);
                        }
                        // pageId = currentDirPage.getPrevPage();
                        // delete empty directory page: (automatically unpinned?)
                        unpinPage(currentDirPageId, false/*undirty*/);
                        freePage(currentDirPageId);
                        // pinPage(pageId, currentDirPage, false);
                
                    } else {
                        // either (the directory page has at least one more datapagerecord
                        // entry) or (it is the first directory page):
                        // in both cases we do not delete it, but we have to unpin it:
                        
                        // unpinPage(currentDirPageId, true /* == DIRTY */);
                    }
                
                }
                if(list.size() == 0){
                    try{
                        unpinPage(currentDataPageId, false);
                        unpinPage(currentDirPageId, false);
                    }catch (Exception e){}
                }
                continue listLoop;

            }
            else{
                while(currentDirPageId.pid != INVALID_PAGE){
                    secondLoop: for(currentDataPageRid = thisPageRid; currentDataPageRid != null; currentDataPageRid = currentDirPage.nextMap(currentDataPageRid)){
                        try{
                            amap = currentDirPage.returnRecord(currentDataPageRid);
                        }catch(InvalidSlotNumberException e){
                            // e.printStackTrace();
                            // System.out.println(currentDataPageRid.pageNo.pid + " "+currentDataPageRid.slotNo + " "+ currentDirPageId.pid);
                            continue secondLoop;
                        }
                        // System.out.println("THIS");
                        dpinfo = new DataPageInfo(amap);
                        // System.out.println(dpinfo.pageId.pid);
                        // currentDataPageId.pid = ConvertMap.getIntValue(amap.getMapOffset()+8, amap.data);
                        if(ConvertMap.getIntValue(amap.getMapOffset()+8, amap.data) == toDelete.pageNo.pid){
                            try{
                                unpinPage(currentDataPageId, false);
                            }catch (Exception e){
                                // e.printStackTrace();
                            }
                            thisPageRid = currentDataPageRid;
                            currentDataPageId.pid = ConvertMap.getIntValue(amap.getMapOffset()+8, amap.data);
                            pinPage(currentDataPageId, currentDataPage, false);
                            currentDataPage.deleteMap(toDelete);
                            byte [] recbyt = new byte[4];
                            byte [] available_space_by = new byte[4];
                            System.arraycopy(amap.data, amap.getMapOffset(), available_space_by, 0, 4);
                            System.arraycopy(amap.data, amap.getMapOffset()+4, recbyt, 0, 4);
                            int recm = ConvertMap.getIntValue(0, recbyt);
                            recm--;
                            if (recm >= 1) {
                                // more records remain on datapage so it still hangs around.  
                                // we just need to modify its directory entry
                                
                                dpinfo.availspace = currentDataPage.available_space();
 
                                // amap = currentDirPage.getMap(currentDataPageRid);
                                ConvertMap.setIntValue(dpinfo.availspace, amap.getMapOffset(), amap.data);
                                ConvertMap.setIntValue(recm, amap.getMapOffset()+4, amap.data);
                                amap = currentDirPage.returnRecord(currentDataPageRid);
                                // System.out.println(Arrays.toString(amap.data));
                                continue listLoop;
                            }else {
                                unpinPage(currentDataPageId, false /*undirty*/);
                            
                                freePage(currentDataPageId);
                                
                                // delete corresponding DataPageInfo-entry on the directory page:
                                // currentDataPageRid points to datapage (from for loop above)
                                
                                currentDirPage.deleteMap(currentDataPageRid);
                                currentDataPageRid = currentDirPage.firstMap();
                                PageId pageId;
                                pageId = currentDirPage.getPrevPage();
                                if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE)) {
                                    // the directory-page is not the first directory page and it is empty:
                                    // delete it
                                    
                                    // point previous page around deleted page:
                                    
                                    BigPage prevDirPage = new BigPage();
                                    pinPage(pageId, prevDirPage, false);
                    
                                    pageId = currentDirPage.getNextPage();
                                    prevDirPage.setNextPage(pageId);
                                    pageId = currentDirPage.getPrevPage();
                                    unpinPage(pageId, true /* = DIRTY */);
                                    
                                    
                                    // set prevPage-pointer of next Page
                                    pageId = currentDirPage.getNextPage();
                                    if(pageId.pid != INVALID_PAGE) {
                                        BigPage nextDirPage = new BigPage();
                                        pageId = currentDirPage.getNextPage();
                                        pinPage(pageId, nextDirPage, false);
                                        
                                        //nextDirPage.openHFpage(apage);
                                        
                                        pageId = currentDirPage.getPrevPage();
                                        nextDirPage.setPrevPage(pageId);
                                        pageId = currentDirPage.getNextPage();
                                        unpinPage(pageId, true /* = DIRTY */);
                                    }
                                    // pageId = currentDirPage.getPrevPage();
                                    // delete empty directory page: (automatically unpinned?)
                                    unpinPage(currentDirPageId, false/*undirty*/);
                                    freePage(currentDirPageId);
                                    // pinPage(pageId, currentDirPage, false);
                                    
                            
                                } else {
                                    // either (the directory page has at least one more datapagerecord
                                    // entry) or (it is the first directory page):
                                    // in both cases we do not delete it, but we have to unpin it:
                                    
                                    // unpinPage(currentDirPageId, true /* == DIRTY */);
                                }
                            }
                            continue listLoop;
                        }
                    }
                    nextDirPageId = currentDirPage.getNextPage();
                    Map ak = currentDirPage.returnRecord(currentDirPage.firstMap());
                    // System.out.println(Arrays.toString(ak.data));
                    try{
                        unpinPage(currentDirPageId, true /*undirty*/);
                        unpinPage(currentDataPageId, true);
                    }
                    catch(Exception e) {
                        // throw new HFException (e, "heapfile,_find,unpinpage failed");
                    }
            
                    currentDirPageId.pid = nextDirPageId.pid;
                    if(currentDirPageId.pid != INVALID_PAGE) {
                        pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
                        thisPageRid = currentDirPage.firstMap();
                        if(currentDirPage == null)
                            throw new HFException(null, "pinPage return null page");  
                    }
                }
            }
        }
        try {
            unpinPage(currentDataPageId, true);
            unpinPage(currentDirPageId, true);
        } catch (Exception e) {
            //TODO: handle exception
        }
        return false;
    }

    public boolean updateMap(MID mid, Map newmap)
            throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {
        boolean status;
        BigPage dirPage = new BigPage();
        PageId currentDirPageId = new PageId();
        BigPage dataPage = new BigPage();
        PageId currentDataPageId = new PageId();
        MID currentDataPageRid = new MID();
        
        status = _findDataPage(mid,
                    currentDirPageId, dirPage, 
                    currentDataPageId, dataPage,
                    currentDataPageRid);
      
        if(status != true) return status;	// record not found
        Map aMap = new Map();
        aMap = dataPage.returnRecord(mid);
        
        // Assume update a record with a record whose length is equal to
        // the original record
        
        // if(newmap.size() != aMap.size())
        // {
        //     unpinPage(currentDataPageId, false /*undirty*/);
        //     unpinPage(currentDirPageId, false /*undirty*/);
            
        //     throw new InvalidUpdateException(null, "invalid record update");
        
        // }

        // new copy of this record fits in old space;
        aMap.mapCopy(newmap);
        unpinPage(currentDataPageId, true /* = DIRTY */);
        
        unpinPage(currentDirPageId, false /*undirty*/);
        
        
        return true;
    }

    public Stream openStream() throws InvalidTupleSizeException, IOException {
        Stream newsStream = new Stream(this);
        return newsStream;
    }

    public String getFileName() {
        return _fileName;
    }

    private void pinPage(PageId pageno, Page page, boolean emptyPage)throws HFBufMgrException {
        
        try {
        SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
        throw new HFBufMgrException(e,"Heapfile.java: pinPage() failed");
        }
        
    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
        throws HFBufMgrException {

        try {
        SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
        throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

    private void freePage(PageId pageno) throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Heapfile.java: freePage() failed");
        }

    } // end of freePage

    private PageId newPage(Page page, int num) throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
        tmpId = SystemDefs.JavabaseBM.newPage(page,num);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage

    private PageId get_file_entry(String filename)
        throws HFDiskMgrException {

        PageId tmpId = new PageId();

        try {
        tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
        }
        catch (Exception e) {
        throw new HFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry

    private void add_file_entry(String filename, PageId pageno)
        throws HFDiskMgrException {

        try {
        SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
        }
        catch (Exception e) {
        throw new HFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private void delete_file_entry(String filename)
        throws HFDiskMgrException {

        try {
        SystemDefs.JavabaseDB.delete_file_entry(filename);
        }
        catch (Exception e) {
        throw new HFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
        }

    } 
}