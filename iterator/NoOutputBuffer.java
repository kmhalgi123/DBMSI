package iterator;

import chainexception.*;

public class NoOutputBuffer extends ChainException {
  /**
   *
   */
  private static final long serialVersionUID = 3503532789847047907L;

  public NoOutputBuffer(String s) {
    super(null, s);
  }
  public NoOutputBuffer(Exception prev, String s){super(prev,s);}
}
