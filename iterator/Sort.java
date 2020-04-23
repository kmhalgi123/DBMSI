package iterator;

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.PriorityQueue;

import global.*;
import bufmgr.*;
import diskmgr.*;
import BigT.*;
import btree.BTIndexPage;
import index.*;
import chainexception.*;

/**
 * The Sort class sorts a file. All necessary information are passed as 
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class Sort extends Iterator implements GlobalConst
{
  private static final int ARBIT_RUNS = 10;
  
  private AttrType[]  _in;         
  private short       n_cols;
  private short[]     str_lens;
  private Iterator    _am;
  private Stream      _st;
  private int         _sort_fld;
  private int         _order_type;
  private MapOrder    order;
  private int         _n_pages;
  private byte[][]    bufs;
  private boolean     first_time;
  private int         Nruns;
  private int         max_elems_in_heap;
  private int         sortFldLen;
  private int         tuple_size;
  private String      current_row;
  private pnodeSplayPQ Q;
  private bigt[]   temp_files; 
  private int          n_tempfiles;
  private Map        output_tuple;  
  private int[]        n_tuples;
  private int          n_runs;
  private Map        op_buf;
  private OBuf         o_buf;
  private SpoofIbuf[]  i_buf;
  private PageId[]     bufs_pids;
  private boolean      isRowSort;
  private PriorityQueue<customPnode> cupq; 
  private String       _colname;
  private ArrayList<pnode> arrayList;
  private int          current_row_max_int = 0;
  private int          current_row_min_int = Integer.MAX_VALUE;
  private int          rowSortOrder;
  private boolean      stJoin = false;
  private PriorityQueue Qs;
  private boolean useBM = false; // flag for whether to use buffer manager
  
  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min)
   * from each run into a heap. <code>delete_min() </code> will then get 
   * the minimum of all runs.
   * @param tuple_size size (in bytes) of each tuple
   * @param n_R_runs number of runs
   * @exception IOException from lower layers
   * @exception LowMemException there is not enough memory to 
   *                 sort in two passes (a subclass of SortException).
   * @exception SortException something went wrong in the lower layer. 
   * @exception Exception other exceptions
   */
  private void setup_for_merge(int tuple_size, int n_R_runs)
    throws IOException, 
	   LowMemException, 
	   SortException,
	   Exception
  {
    // don't know what will happen if n_R_runs > _n_pages
    if (n_R_runs > _n_pages) 
      throw new LowMemException("Sort.java: Not enough memory to sort in two passes."); 

    int i;
    pnode cur_node;  // need pq_defs.java
    
    i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
    for (int j=0; j<n_R_runs; j++) i_buf[j] = new SpoofIbuf();
    
    // construct the lists, ignore TEST for now
    // this is a patch, I am not sure whether it works well -- bingjie 4/20/98
    
    for (i=0; i<n_R_runs; i++) {
      byte[][] apage = new byte[1][];
      apage[0] = bufs[i];

      // need iobufs.java
      try{
      i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_tuples[i]);
      }catch(Exception e){
        System.out.println("Not minimum buffer pages to sort the tables!");
        throw new Exception();
      }

      cur_node = new pnode();
      cur_node.run_num = i;
      
      // may need change depending on whether Get() returns the original
      // or make a copy of the tuple, need io_bufs.java ???
      Map temp_tuple = new Map(tuple_size);

      try {
	      temp_tuple.setHdr(str_lens);
      }
      catch (Exception e) {
	      throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
      }
      
      temp_tuple =i_buf[i].Get(temp_tuple);  // need io_bufs.java
            
      if (temp_tuple != null) {
        /*
        System.out.print("Get tuple from run " + i);
        temp_tuple.print(_in);
        */
        cur_node.map = temp_tuple; // no copy needed
        try {
          Q.enq(cur_node);
        }
        catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        }
        catch (MapUtilsException e) {
          throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
        }

      }
    }
    return; 
  }
  
  /**
   * Generate sorted runs.
   * Using heap sort.
   * @param  max_elems    maximum number of elements in heap
   * @param  sortFldType  attribute type of the sort field
   * @param  sortFldLen   length of the sort field
   * @return number of runs generated
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems, int sortFldLen) 
    throws IOException, 
	   SortException, 
	   UnknowAttrType,
	   MapUtilsException,
	   JoinsException,
	   Exception {
    Map tuple; 
    pnode cur_node;
    pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, order);
    pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, order);
    pnodeSplayPQ pcurr_Q = Q1;
    pnodeSplayPQ pother_Q = Q2; 
    Map lastElem = new Map(tuple_size);  // need tuple.java
    try {
      lastElem.setHdr(str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: setHdr() failed");
    }
    
    int run_num = 0;  // keeps track of the number of runs

    // number of elements in Q
    //    int nelems_Q1 = 0;
    //    int nelems_Q2 = 0;
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    
    int comp_res;
    
    // set the lastElem to be the minimum value for the sort field
    if(order.mapOrder == MapOrder.Ascending) {
      try {
	      MIN_VAL(lastElem, _sort_fld);
      } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
      } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
      } 
    }
    else {
      try {
	      MAX_VAL(lastElem, _sort_fld);
      } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
      } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
      } 
    }
    
    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
	      tuple = _am.get_next();  // according to Iterator.java
      } catch (Exception e) {
        e.printStackTrace(); 
        throw new SortException(e, "Sort.java: get_next() failed");
      } 
      
      if (tuple == null) {
	      break;
      }
      cur_node = new pnode();
      cur_node.map = new Map(tuple); // tuple copy needed --  Bingjie 4/29/98 
      cur_node.map.mapSetup();
      pcurr_Q.enq(cur_node);
      p_elems_curr_Q ++;
    }
    
    // now the queue is full, starting writing to file while keep trying
    // to add new tuples to the queue. The ones that does not fit are put
    // on the other queue temperarily
    while (true) {
      cur_node = pcurr_Q.deq();
      if (cur_node == null) break; 
      p_elems_curr_Q --;
      
      comp_res = MapUtils.CompareMapWithValue(cur_node.map, lastElem, _sort_fld);  // need tuple_utils.java
      
      if ((comp_res < 0 && order.mapOrder == MapOrder.Ascending) || (comp_res > 0 && order.mapOrder== MapOrder.Descending)) {
	      // doesn't fit in current run, put into the other queue
        try {
          pother_Q.enq(cur_node);
        }
        catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        }
        p_elems_other_Q ++;
      }
      else {
        // set lastElem to have the value of the current tuple,
        // need tuple_utils.java
        MapUtils.SetValue(lastElem, cur_node.map, _sort_fld);
        // write tuple to output file, need io_bufs.java, type cast???
        //	System.out.println("Putting tuple into run " + (run_num + 1)); 
        //	cur_node.tuple.print(_in);
	
	      o_buf.Put(cur_node.map);
      }
      
      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
        // close current run and start next run
        n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
        run_num ++;

        // check to see whether need to expand the array
        if (run_num == n_tempfiles) {
          bigt[] temp1 = new bigt[2*n_tempfiles];
          for (int i=0; i<n_tempfiles; i++) {
            temp1[i] = temp_files[i];
          }
          temp_files = temp1; 
          n_tempfiles *= 2; 

          int[] temp2 = new int[2*n_runs];
          for(int j=0; j<n_runs; j++) {
            temp2[j] = n_tuples[j];
          }
          n_tuples = temp2;
          n_runs *=2; 
        }
	
        try {
          temp_files[run_num] = new bigt(null);
        }
        catch (Exception e) {
          throw new SortException(e, "Sort.java: create Heapfile failed");
        }
	
        // need io_bufs.java
        o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
	
        // set the last Elem to be the minimum value for the sort field
        if(order.mapOrder == MapOrder.Ascending) {
          try {
            MIN_VAL(lastElem, _sort_fld);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          } 
        }
        else {
          try {
            MAX_VAL(lastElem, _sort_fld);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          } 
        }
    
        // switch the current heap and the other heap
        pnodeSplayPQ tempQ = pcurr_Q;
        pcurr_Q = pother_Q;
        pother_Q = tempQ;
        int tempelems = p_elems_curr_Q;
        p_elems_curr_Q = p_elems_other_Q;
        p_elems_other_Q = tempelems;
      }
      
      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
	      while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
          try {
            tuple = _am.get_next();  // according to Iterator.java
          } catch (Exception e) {
            throw new SortException(e, "get_next() failed");
          } 
	  
          if (tuple == null) {
            break;
          }
          cur_node = new pnode();
          cur_node.map = new Map(tuple); // tuple copy needed --  Bingjie 4/29/98 
          cur_node.map.mapSetup();
          try {
            pcurr_Q.enq(cur_node);
          }
          catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
          }
          p_elems_curr_Q ++;
        }
      }
      
      // Check if we are done
      if (p_elems_curr_Q == 0) {
        // current queue empty despite our attemps to fill in
        // indicating no more tuples from input
        if (p_elems_other_Q == 0) {
          // other queue is also empty, no more tuples to write out, done
          break; // of the while(true) loop
        }
        else {
          // generate one more run for all tuples in the other queue
          // close current run and start next run
          n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
          run_num ++;
          
          // check to see whether need to expand the array
          if (run_num == n_tempfiles) {
            bigt[] temp1 = new bigt[2*n_tempfiles];
            for (int i=0; i<n_tempfiles; i++) {
              temp1[i] = temp_files[i];
            }
            temp_files = temp1; 
            n_tempfiles *= 2; 
            
            int[] temp2 = new int[2*n_runs];
            for(int j=0; j<n_runs; j++) {
              temp2[j] = n_tuples[j];
            }
            n_tuples = temp2;
            n_runs *=2; 
          }

          try {
            temp_files[run_num] = new bigt(null); 
          }
          catch (Exception e) {
            throw new SortException(e, "Sort.java: create Heapfile failed");
          }
	  
          // need io_bufs.java
          o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
          
          // set the last Elem to be the minimum value for the sort field
          if(order.mapOrder == MapOrder.Ascending) {
            try {
              MIN_VAL(lastElem, _sort_fld);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            } 
          }
          else {
            try {
              MAX_VAL(lastElem, _sort_fld);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            } 
          }
	
          // switch the current heap and the other heap
          pnodeSplayPQ tempQ = pcurr_Q;
          pcurr_Q = pother_Q;
          pother_Q = tempQ;
          int tempelems = p_elems_curr_Q;
          p_elems_curr_Q = p_elems_other_Q;
          p_elems_other_Q = tempelems;
        }
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_tuples[run_num] = (int) o_buf.flush();
    run_num ++;
    
    return run_num; 
  }

  private int generate_runs3(int max_elems, int sortFldLen) 
    throws IOException, 
	   SortException, 
	   UnknowAttrType,
	   MapUtilsException,
	   JoinsException,
	   Exception {
    Map tuple; 
    pnode cur_node;
    pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, order);
    pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, order);
    pnodeSplayPQ pcurr_Q = Q1;
    pnodeSplayPQ pother_Q = Q2; 
    Map lastElem = new Map(tuple_size);  // need tuple.java
    try {
      lastElem.setHdr(str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: setHdr() failed");
    }
    
    int run_num = 0;  // keeps track of the number of runs

    // number of elements in Q
    //    int nelems_Q1 = 0;
    //    int nelems_Q2 = 0;
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    
    int comp_res;
    
    // set the lastElem to be the minimum value for the sort field
    if(order.mapOrder == MapOrder.Ascending) {
      try {
	      MIN_VAL(lastElem, _sort_fld);
      } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
      } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
      } 
    }
    else {
      try {
	      MAX_VAL(lastElem, _sort_fld);
      } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
      } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
      } 
    }
    
    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
	      tuple = _st.getNext(new MID());  // according to Iterator.java
      } catch (Exception e) {
        e.printStackTrace(); 
        throw new SortException(e, "Sort.java: get_next() failed");
      } 
      
      if (tuple == null) {
	      break;
      }
      cur_node = new pnode();
      cur_node.map = new Map(tuple); // tuple copy needed --  Bingjie 4/29/98 
      cur_node.map.mapSetup();
      pcurr_Q.enq(cur_node);
      p_elems_curr_Q ++;
    }
    
    // now the queue is full, starting writing to file while keep trying
    // to add new tuples to the queue. The ones that does not fit are put
    // on the other queue temperarily
    while (true) {
      cur_node = pcurr_Q.deq();
      if (cur_node == null) break; 
      p_elems_curr_Q --;
      
      comp_res = MapUtils.CompareMapWithValue(cur_node.map, lastElem, _sort_fld);  // need tuple_utils.java
      
      if ((comp_res < 0 && order.mapOrder == MapOrder.Ascending) || (comp_res > 0 && order.mapOrder== MapOrder.Descending)) {
	      // doesn't fit in current run, put into the other queue
        try {
          pother_Q.enq(cur_node);
        }
        catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        }
        p_elems_other_Q ++;
      }
      else {
        // set lastElem to have the value of the current tuple,
        // need tuple_utils.java
        MapUtils.SetValue(lastElem, cur_node.map, _sort_fld);
        // write tuple to output file, need io_bufs.java, type cast???
        //	System.out.println("Putting tuple into run " + (run_num + 1)); 
        //	cur_node.tuple.print(_in);
	
	      o_buf.Put(cur_node.map);
      }
      
      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
        // close current run and start next run
        n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
        run_num ++;

        // check to see whether need to expand the array
        if (run_num == n_tempfiles) {
          bigt[] temp1 = new bigt[2*n_tempfiles];
          for (int i=0; i<n_tempfiles; i++) {
            temp1[i] = temp_files[i];
          }
          temp_files = temp1; 
          n_tempfiles *= 2; 

          int[] temp2 = new int[2*n_runs];
          for(int j=0; j<n_runs; j++) {
            temp2[j] = n_tuples[j];
          }
          n_tuples = temp2;
          n_runs *=2; 
        }
	
        try {
          temp_files[run_num] = new bigt(null);
        }
        catch (Exception e) {
          throw new SortException(e, "Sort.java: create Heapfile failed");
        }
	
        // need io_bufs.java
        o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
	
        // set the last Elem to be the minimum value for the sort field
        if(order.mapOrder == MapOrder.Ascending) {
          try {
            MIN_VAL(lastElem, _sort_fld);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          } 
        }
        else {
          try {
            MAX_VAL(lastElem, _sort_fld);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          } 
        }
    
        // switch the current heap and the other heap
        pnodeSplayPQ tempQ = pcurr_Q;
        pcurr_Q = pother_Q;
        pother_Q = tempQ;
        int tempelems = p_elems_curr_Q;
        p_elems_curr_Q = p_elems_other_Q;
        p_elems_other_Q = tempelems;
      }
      
      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
	      while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
          try {
            tuple = _st.getNext(new MID());  // according to Iterator.java
          } catch (Exception e) {
            throw new SortException(e, "get_next() failed");
          } 
	  
          if (tuple == null) {
            break;
          }
          cur_node = new pnode();
          cur_node.map = new Map(tuple); // tuple copy needed --  Bingjie 4/29/98 
          cur_node.map.mapSetup();
          try {
            pcurr_Q.enq(cur_node);
          }
          catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
          }
          p_elems_curr_Q ++;
        }
      }
      
      // Check if we are done
      if (p_elems_curr_Q == 0) {
        // current queue empty despite our attemps to fill in
        // indicating no more tuples from input
        if (p_elems_other_Q == 0) {
          // other queue is also empty, no more tuples to write out, done
          break; // of the while(true) loop
        }
        else {
          // generate one more run for all tuples in the other queue
          // close current run and start next run
          n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
          run_num ++;
          
          // check to see whether need to expand the array
          if (run_num == n_tempfiles) {
            bigt[] temp1 = new bigt[2*n_tempfiles];
            for (int i=0; i<n_tempfiles; i++) {
              temp1[i] = temp_files[i];
            }
            temp_files = temp1; 
            n_tempfiles *= 2; 
            
            int[] temp2 = new int[2*n_runs];
            for(int j=0; j<n_runs; j++) {
              temp2[j] = n_tuples[j];
            }
            n_tuples = temp2;
            n_runs *=2; 
          }

          try {
            temp_files[run_num] = new bigt(null); 
          }
          catch (Exception e) {
            throw new SortException(e, "Sort.java: create Heapfile failed");
          }
	  
          // need io_bufs.java
          o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
          
          // set the last Elem to be the minimum value for the sort field
          if(order.mapOrder == MapOrder.Ascending) {
            try {
              MIN_VAL(lastElem, _sort_fld);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            } 
          }
          else {
            try {
              MAX_VAL(lastElem, _sort_fld);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            } 
          }
	
          // switch the current heap and the other heap
          pnodeSplayPQ tempQ = pcurr_Q;
          pcurr_Q = pother_Q;
          pother_Q = tempQ;
          int tempelems = p_elems_curr_Q;
          p_elems_curr_Q = p_elems_other_Q;
          p_elems_other_Q = tempelems;
        }
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_tuples[run_num] = (int) o_buf.flush();
    run_num ++;
    
    return run_num; 
  }
  
  private int generate_runs2(int max_elems, int sortFldLen)  throws IOException, 
    SortException, 
    UnknowAttrType,
    MapUtilsException,
    JoinsException,
    Exception{
    Map tuple; 
    Hashtable<String, ArrayList<pnode>> hashtable = new Hashtable<>();
    pnode cur_node, prev_node = null;
    pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, order);
    pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, order);
    pnodeSplayPQ secQ = new pnodeSplayPQ();
    pnodeSplayPQ pcurr_Q = Q1;
    pnodeSplayPQ pother_Q = Q2; 
    Map lastElem = new Map(tuple_size);  // need tuple.java
    try {
      lastElem.setHdr(str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: setHdr() failed");
    }
    
    int run_num = 0;  // keeps track of the number of runs

    // number of elements in Q
    //    int nelems_Q1 = 0;
    //    int nelems_Q2 = 0;
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    
    int comp_res;
    
    // set the lastElem to be the minimum value for the sort field
    if(order.mapOrder == MapOrder.Ascending) {
      try {
	      MIN_VAL(lastElem, _sort_fld);
      } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
      } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
      } 
    }
    else {
      try {
	      MAX_VAL(lastElem, _sort_fld);
      } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
      } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
      } 
    }
    // System.out.println(max_elems);
    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
	      tuple = _am.get_next();  // according to Iterator.java
      } catch (Exception e) {
        e.printStackTrace(); 
        throw new SortException(e, "Sort.java: get_next() failed");
      } 
      
      if (tuple == null) {
	      break;
      }
      cur_node = new pnode();
      cur_node.map = new Map(tuple); // tuple copy needed --  Bingjie 4/29/98 
      cur_node.map.mapSetup();
      pcurr_Q.enq(cur_node);
      p_elems_curr_Q ++;     
    }

    while(true){
      cur_node = pcurr_Q.deq();
      if (cur_node == null) {
        hashtable.put(current_row, arrayList);
        cupq.add(new customPnode(prev_node, current_row_max_int));
        break;
      }
      Map map = cur_node.map;
      map.mapSetup();
      if (current_row.equals("None")){
        current_row = map.getRowLabel();
        if(map.getColumnLabel().equals(_colname)){
          current_row_max_int = map.getTimeStamp();
        }
        prev_node = cur_node;
        arrayList.add(cur_node);
        continue;
      }else if(current_row.equals(map.getRowLabel())){
        if(map.getColumnLabel().equals(_colname) && map.getTimeStamp() > current_row_max_int){
          current_row_max_int = map.getTimeStamp();
        }
        arrayList.add(cur_node);
        prev_node = cur_node;
        continue; ///////// Could be error here
      }else {
        hashtable.put(current_row,arrayList);
        current_row = map.getRowLabel();
        cupq.add(new customPnode(prev_node, current_row_max_int));
        current_row_max_int = 0;
        arrayList= new ArrayList<>();
        pcurr_Q.enq(cur_node);
      }
    }
    // now the queue is full, starting writing to file while keep trying
    // to add new tuples to the queue. The ones that does not fit are put
    // on the other queue temperarily
    while (true) {
      customPnode cPnode = null;
      cPnode = cupq.poll();
      if(cPnode == null) break;
      cur_node = cPnode.getPnode();
      Map map = cur_node.map;
      map.mapSetup();
      ArrayList<pnode> arrayList2 = hashtable.get(map.getRowLabel());
      // System.out.println(arrayList2.size());
      p_elems_curr_Q -= arrayList2.size();
      // while(!arrayList2.isEmpty())

      comp_res = MapUtils.CompareMapWithValue(cur_node.map, lastElem, _sort_fld);  // need tuple_utils.java
      if ((comp_res < 0 && order.mapOrder == MapOrder.Ascending) || (comp_res > 0 && order.mapOrder== MapOrder.Descending)) {
	      // doesn't fit in current run, put into the other queue
        try {
          for(int i=0;i<arrayList2.size();i++){
            pother_Q.enq(arrayList2.get(i));
          }
        }
        catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        }
        p_elems_other_Q += arrayList2.size();
      }
      else {
        // set lastElem to have the value of the current tuple,
        // need tuple_utils.java
        // cur_node = priorityQueue.peek();
        // MapUtils.SetValue(lastElem, cur_node.map, _sort_fld);
        // write tuple to output file, need io_bufs.java, type cast???
        //	System.out.println("Putting tuple into run " + (run_num + 1)); 
        //	cur_node.tuple.print(_in);
        for(int i=0;i<arrayList2.size();i++){
          cur_node = arrayList2.get(i);
          o_buf.Put(cur_node.map);
        }
	      
      }
      
      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
        // close current run and start next run
        n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
        run_num ++;

        // check to see whether need to expand the array
        if (run_num == n_tempfiles) {
          bigt[] temp1 = new bigt[2*n_tempfiles];
          for (int i=0; i<n_tempfiles; i++) {
            temp1[i] = temp_files[i];
          }
          temp_files = temp1; 
          n_tempfiles *= 2; 

          int[] temp2 = new int[2*n_runs];
          for(int j=0; j<n_runs; j++) {
            temp2[j] = n_tuples[j];
          }
          n_tuples = temp2;
          n_runs *=2; 
        }
	
        try {
          temp_files[run_num] = new bigt(null);
        }
        catch (Exception e) {
          throw new SortException(e, "Sort.java: create Heapfile failed");
        }
	
        // need io_bufs.java
        o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
	
        // set the last Elem to be the minimum value for the sort field
        if(order.mapOrder == MapOrder.Ascending) {
          try {
            MIN_VAL(lastElem, _sort_fld);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          } 
        }
        else {
          try {
            MAX_VAL(lastElem, _sort_fld);
          } catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
          } catch (Exception e) {
            throw new SortException(e, "MIN_VAL failed");
          } 
        }
    
        // switch the current heap and the other heap
        pnodeSplayPQ tempQ = secQ;
        secQ = pother_Q;
        pother_Q = tempQ;
        int tempelems = p_elems_curr_Q;
        p_elems_curr_Q = p_elems_other_Q;
        p_elems_other_Q = tempelems;
      }
      
      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
	      while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
          try {
            tuple = _am.get_next();  // according to Iterator.java
          } catch (Exception e) {
            throw new SortException(e, "get_next() failed");
          } 
	  
          if (tuple == null) {
            break;
          }
          cur_node = new pnode();
          cur_node.map = new Map(tuple); // tuple copy needed --  Bingjie 4/29/98 
          cur_node.map.mapSetup();
          try {
            pcurr_Q.enq(cur_node);
          }
          catch (UnknowAttrType e) {
            throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
          }
          p_elems_curr_Q ++;
        }
      }
      
      // Check if we are done
      if (p_elems_curr_Q == 0) {
        // current queue empty despite our attemps to fill in
        // indicating no more tuples from input
        if (p_elems_other_Q == 0) {
          // other queue is also empty, no more tuples to write out, done
          break; // of the while(true) loop
        }
        else {
          // generate one more run for all tuples in the other queue
          // close current run and start next run
          n_tuples[run_num] = (int) o_buf.flush();  // need io_bufs.java
          run_num ++;
          
          // check to see whether need to expand the array
          if (run_num == n_tempfiles) {
            bigt[] temp1 = new bigt[2*n_tempfiles];
            for (int i=0; i<n_tempfiles; i++) {
              temp1[i] = temp_files[i];
            }
            temp_files = temp1; 
            n_tempfiles *= 2; 
            
            int[] temp2 = new int[2*n_runs];
            for(int j=0; j<n_runs; j++) {
              temp2[j] = n_tuples[j];
            }
            n_tuples = temp2;
            n_runs *=2; 
          }

          try {
            temp_files[run_num] = new bigt(null); 
          }
          catch (Exception e) {
            throw new SortException(e, "Sort.java: create Heapfile failed");
          }
	  
          // need io_bufs.java
          o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
          
          // set the last Elem to be the minimum value for the sort field
          if(order.mapOrder == MapOrder.Ascending) {
            try {
              MIN_VAL(lastElem, _sort_fld);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            } 
          }
          else {
            try {
              MAX_VAL(lastElem, _sort_fld);
            } catch (UnknowAttrType e) {
              throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
              throw new SortException(e, "MIN_VAL failed");
            } 
          }
	
          // switch the current heap and the other heap
          pnodeSplayPQ tempQ = pcurr_Q;
          pcurr_Q = pother_Q;
          pother_Q = tempQ;
          int tempelems = p_elems_curr_Q;
          p_elems_curr_Q = p_elems_other_Q;
          p_elems_other_Q = tempelems;
        }
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_tuples[run_num] = (int) o_buf.flush();
    run_num ++;
    
    return run_num; 
  }
  /**
   * Remove the minimum value among all the runs.
   * @return the minimum tuple removed
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  private Map delete_min() 
    throws IOException, 
	   SortException,
	   Exception
  {
    pnode cur_node;                // needs pq_defs.java  
    Map new_tuple, old_tuple;  

    cur_node = Q.deq();
    old_tuple = cur_node.map;
    /*
    System.out.print("Get ");
    old_tuple.print(_in);
    */
    // we just removed one tuple from one run, now we need to put another
    // tuple of the same run into the queue
    if (i_buf[cur_node.run_num].empty() != true) { 
      // run not exhausted 
      new_tuple = new Map(tuple_size); // need tuple.java??

      try {
	      new_tuple.setHdr(str_lens);
      }
      catch (Exception e) {
	      throw new SortException(e, "Sort.java: setHdr() failed");
      }
      
      new_tuple = i_buf[cur_node.run_num].Get(new_tuple);  
      if (new_tuple != null) {
        /*
        System.out.print(" fill in from run " + cur_node.run_num);
        new_tuple.print(_in);
        */
        cur_node.map = new_tuple;  // no copy needed -- I think Bingjie 4/22/98
        try {
          Q.enq(cur_node);
        } catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        } catch (MapUtilsException e) {
          throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
        } 
      }
      else {
	      throw new SortException("********** Wait a minute, I thought input is not empty ***************");
      }
      
    }

    // changed to return Tuple instead of return char array ????
    return old_tuple; 
  }
  
  /**
   * Set lastElem to be the minimum value of the appropriate type
   * @param lastElem the tuple
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MIN_VAL(Map lastElem, int order_type) 
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MIN_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    // System.out.println("Sort F: "+order_type);
    switch (order_type) {
      case 5: 
        //      lastElem.setHdr(fld_no, junk, null);
        lastElem.setTimeStamp(Integer.MIN_VALUE);
        break;
      case 1:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(1, s);
        break;
      case 2:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(2, s);
        break;
      case 3:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(1, s);
        break;
      case 4:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(2, s);
        break;
      default:
        // don't know how to handle attrSymbol, attrNull
        //System.err.println("error in sort.java");
        throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    
    return;
  }

  /**
   * Set lastElem to be the maximum value of the appropriate type
   * @param lastElem the tuple
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MAX_VAL(Map lastElem, int order_type) 
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MAX_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    
    switch (order_type) {
      case 5: 
        //      lastElem.setHdr(fld_no, junk, null);
        lastElem.setTimeStamp(Integer.MAX_VALUE);
        break;
      case 1:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(1, s);
        break;
      case 2:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(2, s);
        break;
      case 3:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(1, s);
        break;
      case 4:
        //      lastElem.setHdr(fld_no, junk, s_size);
        lastElem.setStrFld(2, s);
        break;
      default:
        // don't know how to handle attrSymbol, attrNull
        //System.err.println("error in sort.java");
        throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    
    return;
  }
  
  /** 
   * Class constructor, take information about the tuples, and set up 
   * the sorting
   * @param in array containing attribute types of the relation
   * @param len_in number of columns in the relation
   * @param str_sizes array of sizes of string attributes
   * @param am an iterator for accessing the tuples
   * @param sort_fld the field number of the field to sort on
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param sort_field_len the length of the sort field
   * @param n_pages amount of memory (in pages) available for sorting
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public Sort( short[]    str_sizes,
	      Iterator   am,                 
	      int        sort_fld,          
	      MapOrder   sort_order,     
	      int        sort_fld_len,  
	      int        n_pages,      
        int        order_type
        ) throws IOException, SortException
  {
    isRowSort = false;
    AttrType[] in = {new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString)};
    _in = new AttrType[4];
    current_row = "None";
    n_cols = 4;
    int n_strs = 3;

    for (int i=0; i<4; i++) {
      _in[i] = new AttrType(in[i].attrType);
    }
    
    str_lens = new short[n_strs];
    
    n_strs = 0;
    for (int i=0; i<4; i++) {
      if (_in[i].attrType == AttrType.attrString) {
        str_lens[n_strs] = str_sizes[n_strs];
        n_strs ++;
      }
    }
    
    Map t = new Map(); // need Tuple.java
    try {
      t.setHdr(str_sizes);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: t.setHdr() failed");
    }
    tuple_size = t.size();
    
    _am = am;
    _sort_fld = sort_fld;
    _order_type = order_type;
    if(_order_type == 1 || _order_type == 3 || _order_type == 6 || _order_type == 7 ){
      _sort_fld = 1;
    }else if(_order_type == 2 || _order_type == 4 || _order_type == 8){
      _sort_fld = 2;
    }else if(_order_type == 5){
      _sort_fld = 3;
    }
    order = sort_order;
    _n_pages = n_pages;
    
    // this may need change, bufs ???  need io_bufs.java
    //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
    bufs_pids = new PageId[_n_pages];
    bufs = new byte[_n_pages][];

    if (useBM) {
      try {
	      get_buffer_pages(_n_pages, bufs_pids, bufs);
      }
      catch (Exception e) {
	      throw new SortException(e, "Sort.java: BUFmgr error");
      }
    }
    else {
      for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
    }
    
    first_time = true;
    
    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new bigt[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_tuples = new int[ARBIT_RUNS]; 
    n_runs = ARBIT_RUNS;

    try {
      temp_files[0] = new bigt(null);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: Heapfile error");
    }
    
    o_buf = new OBuf();
    
    o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
    //    output_tuple = null;
    
    max_elems_in_heap = 200;
    sortFldLen = sort_fld_len;
    
    Q = new pnodeSplayPQ(order_type, order);

    op_buf = new Map(tuple_size);   // need Tuple.java
    try {
      op_buf.setHdr(str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
    }
  }

  public Sort(Iterator   am,                 
	      int        sort_fld,          
	      MapOrder   sort_order,     
	      int        sort_fld_len,  
	      int        n_pages,      
        int        order_type,
        String     colname,
        String     filename
        ) throws IOException, SortException, HFDiskMgrException, HFBufMgrException, HFException, InvalidSlotNumberException
  {
    arrayList = new ArrayList<pnode>();
    cupq = new PriorityQueue<customPnode>(10, new MapConstructor3(sort_order.mapOrder));
    rowSortOrder = sort_order.mapOrder;
    _colname = colname;
    isRowSort = true;
    short[] str_sizes = {32,32,32};
    AttrType[] in = {new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString)};
    _in = new AttrType[4];
    current_row = "None";
    n_cols = 4;
    int n_strs = 3;
    bigt f = new bigt(filename);
    for (int i=0; i<4; i++) {
      _in[i] = new AttrType(in[i].attrType);
    }
    
    str_lens = new short[n_strs];
    
    n_strs = 0;
    for (int i=0; i<4; i++) {
      if (_in[i].attrType == AttrType.attrString) {
        str_lens[n_strs] = str_sizes[n_strs];
        n_strs ++;
      }
    }
    
    Map t = new Map(); // need Tuple.java
    try {
      t.setHdr(str_sizes);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: t.setHdr() failed");
    }
    tuple_size = t.size();
    
    _am = am;
    _sort_fld = sort_fld;
    _order_type = order_type;
    if(_order_type == 1 || _order_type == 3 || _order_type == 6 || _order_type == 7 ){
      _sort_fld = 1;
    }else if(_order_type == 2 || _order_type == 4 || _order_type == 8){
      _sort_fld = 2;
    }else if(_order_type == 5){
      _sort_fld = 3;
    }
    order = new MapOrder(MapOrder.Ascending);
    _n_pages = n_pages;
    
    // this may need change, bufs ???  need io_bufs.java
    //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
    bufs_pids = new PageId[_n_pages];
    bufs = new byte[_n_pages][];

    if (useBM) {
      try {
	      get_buffer_pages(_n_pages, bufs_pids, bufs);
      }
      catch (Exception e) {
	      throw new SortException(e, "Sort.java: BUFmgr error");
      }
    }
    else {
      for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
    }
    
    first_time = true;
    
    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new bigt[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_tuples = new int[ARBIT_RUNS]; 
    n_runs = ARBIT_RUNS;

    try {
      temp_files[0] = new bigt(null);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: Heapfile error");
    }
    
    o_buf = new OBuf();
    
    o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
    //    output_tuple = null;
    
    max_elems_in_heap = f.getMapCnt(); //////////////////// sdasdad
    sortFldLen = sort_fld_len;
    
    Q = new pnodeSplayPQ(order_type, order);

    op_buf = new Map(tuple_size);   // need Tuple.java
    try {
      op_buf.setHdr(str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
    }
  }
  
  public Sort(Stream   am,                 
	      int        sort_fld,          
	      MapOrder   sort_order,     
	      int        sort_fld_len,  
	      int        n_pages,      
        int        order_type
        ) throws IOException, SortException, HFDiskMgrException, HFBufMgrException, HFException, InvalidSlotNumberException
  {
    arrayList = new ArrayList<pnode>();
    cupq = new PriorityQueue<customPnode>(10, new MapConstructor3(sort_order.mapOrder));
    rowSortOrder = sort_order.mapOrder;
    isRowSort = true;
    stJoin = true;
    short[] str_sizes = {32,32,32};
    AttrType[] in = {new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString)};
    _in = new AttrType[4];
    current_row = "None";
    n_cols = 4;
    int n_strs = 3;
    for (int i=0; i<4; i++) {
      _in[i] = new AttrType(in[i].attrType);
    }
    
    str_lens = new short[n_strs];
    
    n_strs = 0;
    for (int i=0; i<4; i++) {
      if (_in[i].attrType == AttrType.attrString) {
        str_lens[n_strs] = str_sizes[n_strs];
        n_strs ++;
      }
    }
    
    Map t = new Map(); // need Tuple.java
    try {
      t.setHdr(str_sizes);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: t.setHdr() failed");
    }
    tuple_size = t.size();
    
    _st = am;
    _sort_fld = sort_fld;
    _order_type = order_type;
    if(_order_type == 1 || _order_type == 3 || _order_type == 6 || _order_type == 7 ){
      _sort_fld = 1;
    }else if(_order_type == 2 || _order_type == 4 || _order_type == 8){
      _sort_fld = 2;
    }else if(_order_type == 5){
      _sort_fld = 3;
    }
    order = new MapOrder(MapOrder.Ascending);
    _n_pages = n_pages;
    
    // this may need change, bufs ???  need io_bufs.java
    //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
    bufs_pids = new PageId[_n_pages];
    bufs = new byte[_n_pages][];

    if (useBM) {
      try {
	      get_buffer_pages(_n_pages, bufs_pids, bufs);
      }
      catch (Exception e) {
	      throw new SortException(e, "Sort.java: BUFmgr error");
      }
    }
    else {
      for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
    }
    
    first_time = true;
    
    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new bigt[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_tuples = new int[ARBIT_RUNS]; 
    n_runs = ARBIT_RUNS;

    try {
      temp_files[0] = new bigt(null);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: Heapfile error");
    }
    
    o_buf = new OBuf();
    
    o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
    //    output_tuple = null;
    
    max_elems_in_heap = 200; //////////////////// sdasdad
    sortFldLen = sort_fld_len;
    
    Q = new pnodeSplayPQ(order_type, order);

    op_buf = new Map(tuple_size);   // need Tuple.java
    try {
      op_buf.setHdr(str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
    }
  }
  
  /**
   * Returns the next tuple in sorted order.
   * Note: You need to copy out the content of the tuple, otherwise it
   *       will be overwritten by the next <code>get_next()</code> call.
   * @return the next tuple, null if all tuples exhausted
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>generate_runs()</code>.
   * @exception UnknowAttrType attribute type unknown
   * @exception LowMemException memory low exception
   * @exception Exception other exceptions
   */
  public Map get_next() 
    throws IOException, 
	   SortException, 
	   UnknowAttrType,
	   LowMemException, 
	   JoinsException,
	   Exception
  {
    if (first_time) {
      // first get_next call to the sort routine
      first_time = false;
      
      if(stJoin){
        Nruns = generate_runs3(max_elems_in_heap, sortFldLen);
      }else{
        // generate runs
        if(isRowSort){
          Nruns = generate_runs2(max_elems_in_heap, sortFldLen);
        }else{
          Nruns = generate_runs(max_elems_in_heap, sortFldLen);
        }
      }
      
      //      System.out.println("Generated " + Nruns + " runs");
      
      // setup state to perform merge of runs. 
      // Open input buffers for all the input file
      setup_for_merge(tuple_size, Nruns);
    }
    
    if (Q.empty()) {  
      // no more tuples availble
      return null;
    }
    
    output_tuple = delete_min();
    if (output_tuple != null){
      op_buf.mapCopy(output_tuple);
      return op_buf; 
    }
    else 
      return null; 
  }

  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public void close() throws SortException, IOException
  {
    // clean up
    if (!closeFlag) {
       
      try {
	      _am.close();
      }
      catch (Exception e) {
	      // throw new SortException(e, "Sort.java: error in closing iterator.");
      }

      if (useBM) {
        try {
          free_buffer_pages(_n_pages, bufs_pids);
        } 
        catch (Exception e) {
          throw new SortException(e, "Sort.java: BUFmgr error");
        }
        for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
      }
      
      for (int i = 0; i<temp_files.length; i++) {
        if (temp_files[i] != null) {
          try {
            temp_files[i].deleteBigt();
          }
          catch (Exception e) {
            throw new SortException(e, "Sort.java: Heapfile error");
          }
          temp_files[i] = null; 
        }
      }
      closeFlag = true;
    } 
  } 

}


