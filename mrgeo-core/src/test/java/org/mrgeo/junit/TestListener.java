/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.junit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.mrgeo.utils.LeakChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

@SuppressWarnings("all") // test code, not included in production
public class TestListener extends RunListener
{  
  private static final Logger log = LoggerFactory.getLogger(TestListener.class);

  private static boolean profile = false;
  private static boolean failfast = false;

  private static void line(StringBuilder builder, final char ch, final int length)
  {
    for (int i = 0; i < length; i++)
    {
      builder.append(ch);
    }
    builder.append('\n');
  }

  private static void line(StringBuilder builder, final char ch1, final char ch2, final int length)
  {
    builder.append(ch1);
    for (int i = 0; i < length - 2; i++)
    {
      builder.append(ch2);
    }
    builder.append(ch1);
    builder.append('\n');
  }

  private static void line(StringBuilder builder, final char ch, final String text, final int length)
  {
    builder.append(ch);
    builder.append(" ");
    builder.append(String.format("%-" + (length - 4) + "s" , text));
    builder.append(" ");
    builder.append(ch);
    builder.append('\n');
  }

  private static void box(final String text, final String lengther)
  {
    StringBuilder builder = new StringBuilder();
    final int length;
    if (lengther != null && lengther.length() > text.length())
    {
      length = lengther.length() + 4;
    }
    else
    {
      length = text.length() + 4;
    }

    line(builder, '*', length);
    line(builder, '*', ' ', length);
    line(builder, '*', text, length);
    line(builder, '*', ' ', length);

    line(builder, '*', length);

    System.out.println(builder.toString());
  }

  private static void box(final String text)
  {
    box(text, null);
  }

  private static void doublebox(final String line1, final String line2)
  {
    StringBuilder builder = new StringBuilder();
    final int length;
    if (line2 != null && line2.length() > line1.length())
    {
      length = line2.length() + 4;
    }
    else
    {
      length = line1.length() + 4;
    }

    line(builder, '*', length);
    line(builder, '*', ' ', length);
    line(builder, '*', line1, length);
    line(builder, '*', ' ', length);
    line(builder, '*', length);
    line(builder, '*', line2, length);
    line(builder, '*', length);

    System.out.println(builder.toString());
  }


  @SuppressFBWarnings(value = "DM_GC", justification = "Used for testing")
  private static void memorybox(final String line1, final String line2) throws Exception
  {
    StringBuilder builder = new StringBuilder();
    final int length;
    if (line2 != null && line2.length() > line1.length())
    {
      length = line2.length() + 4;
    }
    else
    {
      length = line1.length() + 4;
    }

    line(builder, '*', length);
    line(builder, '*', ' ', length);
    line(builder, '*', line1, length);
    line(builder, '*', ' ', length);
    line(builder, '*', length);
    if (line2 != null)
    {
      line(builder, '*', line2, length);
      line(builder, '*', length);
    }

    Runtime r = Runtime.getRuntime();
    double free = r.freeMemory() / (1024.0 * 1024.0);
    double max = r.maxMemory() / (1024.0 * 1024.0);
    double total = r.totalMemory() / (1024.0 * 1024.0);

    System.gc();
    Thread.sleep(1000);

    double freegc = r.freeMemory() / (1024.0 * 1024.0);
    double totalgc = r.totalMemory() / (1024.0 * 1024.0);

    line(builder, '*', "max:       " + String.format("%7.2f mb", max), length);
    line(builder, '*', "allocated: " + String.format("%7.2f mb", total), length);
    line(builder, '*', "free:      " + String.format("%7.2f mb", free), length);
    line(builder, '*', "used:      " + String.format("%7.2f mb", total - free), length);
    line(builder, '*', "after gc:  " + String.format("%7.2f mb", totalgc - freegc), length);


    line(builder, '*', length);

    System.out.println(builder.toString());

  }

  public static int checkOpenHadoopHandles()
  {
    int openhandles = 0;
    try
    {

      int pid = Integer.parseInt(new File("/proc/self").getCanonicalFile().getName());

      Process p = Runtime.getRuntime().exec("lsof -p " + pid + "");

      try (BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream())))
      {
        String line = stdout.readLine();
        while (line != null)
        {
          String[] args = line.split("\\s+");
          if (args.length >= 9)
          {
            String file = args[8];

            if (file.contains(":50010"))
            {
              openhandles++;
            }
          }

          line = stdout.readLine();
        }
        p.getErrorStream().close();
        p.getInputStream().close();
        p.getOutputStream().close();
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }

    return openhandles;
  }

  public static boolean leakcheck(String name)
  {
    Map<Integer, String> leaks = LeakChecker.instance().getAll();

    if (!leaks.isEmpty())
    {
      for (Map.Entry<Integer, String> kv: leaks.entrySet())
      {
        log.error("Leak found!...: " + Integer.toHexString(kv.getKey()) + "\n" + kv.getValue());
      }
        Assert.fail("There were leaks in the system!  See stack traces...");

      return true;
    }

    if (checkOpenHadoopHandles() > 0)
    {
      log.error("Hadoop filesystem resource leak found!...: " + name + " seems to be keeping a connection the the namenode open (an FSDataInputStream or FSDataOutputStream)...");
      Assert.fail("There were Hadoop filesystem resource leaks in the system!  See error log...");
      return true;
    }

    return false;
  }

  @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "Setup tests")
  @Override
  public void testRunStarted(Description description) throws Exception 
  {
    // set the property to profile...
//    System.setProperty("mrgeo.profile", "true");
//    System.setProperty("mrgeo.failfast", "true");

    profile = Boolean.parseBoolean(System.getProperty("mrgeo.profile", "false"));
    failfast = Boolean.parseBoolean(System.getProperty("mrgeo.failfast", "false"));

    if (profile)
    {
      int pid = Integer.parseInt(new File("/proc/self").getCanonicalFile().getName());
      doublebox("Setting up leak detection", "pid: " + pid);

      //
      //      System.out.println("sleeping...");
      //      Thread.sleep(1000 * 5);
    }
  }

  @Override
  public void testRunFinished(Result result) throws Exception 
  {
    if (profile)
    {
      box("Leak detection garbage collection");
      // force a garbage collection to see if there are any leaks.
      System.gc();
      Thread.sleep(2000);

      System.runFinalization();
      Thread.sleep(1000);

      //    System.out.println("sleeping...");
      //    Thread.sleep(1000 * 20);

      memorybox("Leak detection                    ", null);
      leakcheck("End of run\n");
    }
  }

  @SuppressFBWarnings(value = "DM_EXIT", justification = "Force fast exit (fail fast)")
  @Override
  public void testFailure(Failure failure) throws Exception 
  {
    if (failfast)
    {
      System.err.println("FAILURE: " + failure);

      String[] trace = failure.getTrace().split("\n");
      for (String line: trace)
      {
        System.err.println("   " + line);
      }
      System.exit(-1);
    }
  }

  @Override
  public void testFinished(Description description) throws Exception
  {
    super.testFinished(description);

    if (profile)
    {
      memorybox("Test Finished", description.getDisplayName());
      box("Leak detection                    ", null);
      leakcheck(description.getDisplayName());
    }
    else
    {
      doublebox("Test Finished", description.getDisplayName());
    }
  }

  @Override
  public void testStarted(Description description) throws Exception
  {
    super.testStarted(description);

    doublebox("Starting", description.getDisplayName());
  }
}
