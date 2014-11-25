/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Date;

public final class Logger extends PrintStream
{
  private static boolean created = false;
  private static PrintStream log = null;
  private static String logfile = null;

  @SuppressWarnings("hiding")
  public static synchronized void createLogger(String logfile, boolean append)
      throws FileNotFoundException
  {
    if (created)
      return;
    Logger.logfile = logfile;
    Logger logger = new Logger(System.out, logfile, append);
    System.setOut(logger);
    System.setErr(logger);
    created = true;
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        System.out.flush();
      }
    });
  }

  public static String getLogFile()
  {
    return logfile;
  }

  public static void writeBanner()
  {
    System.out.println(StringUtils.pad(80, '#'));
    Date d = new Date();
    System.out.println(d.toString());
    System.out.println(StringUtils.pad(80, '#'));
  }

  public static void writeBanner(String s)
  {
    System.out.println(StringUtils.pad(80, '#'));
    Date d = new Date();
    System.out.println(s);
    System.out.println(d.toString());
    System.out.println(StringUtils.pad(80, '#'));
  }

  public Logger(PrintStream out, String logfile, boolean append) throws FileNotFoundException
  {
    super(out);
    log = new PrintStream(new BufferedOutputStream(new FileOutputStream(logfile, append), 4096));
  }

  @Override
  protected void finalize()
  {
    flush();
  }

  @Override
  public void flush()
  {
    super.flush();
    log.flush();
  }

  @Override
  public void write(byte buf[], int off, int len)
  {
    try
    {
      super.write(buf, off, len);
      log.write(buf, off, len);
    }
    catch (Exception e)
    {
    }
  }
}
