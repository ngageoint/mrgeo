package org.mrgeo.format;

import org.apache.hadoop.fs.FSDataInputStream;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.util.LinkedList;

public abstract class SaxContentHandler<KEY, VALUE> extends DefaultHandler
{
  public class Pair
  {
    KEY key;
    VALUE value;

    Pair(KEY k, VALUE v)
    {
      key = k;
      value = v;
    }
  }

  private LinkedList<Pair> deque = new LinkedList<Pair>();
  private volatile boolean done = false;
  private FSDataInputStream fdis = null;
  private long stopOffset;

  public SaxContentHandler()
  {

  }

  @SuppressWarnings("hiding")
  public void init(FSDataInputStream is, long stopOffset)
  {
    fdis = is;
    this.stopOffset = stopOffset;
  }

  public synchronized void addPair(KEY k, VALUE v) throws IOException,
      DoneSaxException
  {
    synchronized (this)
    {
      deque.add(new Pair(k, v));
      notifyAll();
    }
    checkDone();
  }

  public synchronized void checkDone() throws DoneSaxException, IOException
  {
    if (fdis.getPos() >= stopOffset)
    {
      done();
      throw new DoneSaxException();
    }
  }

  public synchronized void done()
  {
    done = true;
    notifyAll();
  }

  public synchronized Pair getPair() throws InterruptedException
  {
    while (done == false || deque.size() > 0)
    {
      if (deque.size() > 0)
      {
        return deque.pop();
      }
      wait();
    }
    return null;
  }
}
