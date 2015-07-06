/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.cmd;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.Permission;

public class MrGeoTest extends LocalRunnerTest
{
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  
  MrGeo mrgeo = new MrGeo();
  private SecurityManager sm;
  ByteArrayOutputStream output;

  /********************
   * This sets up JUnit to be able to test the System.exit() value...
   */
  protected static class ExitException extends SecurityException 
  {
    private static final long serialVersionUID = 1L;
    public final int status;

    public ExitException(int status) {
      super("" + status);
      this.status = status;
    }
    
    @Override
    public void printStackTrace()
    {
      // no-op
    }
  }

  private static class CaptureExitSecurityManager extends SecurityManager 
  {
    public CaptureExitSecurityManager()
    {
      super();
    }

    @Override
    public void checkPermission(Permission perm) 
    {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) 
    {
      // allow anything.
    }

    @Override
    public void checkExit(int status) 
    {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }
  
  public static class FakeCommandSpi extends CommandSpi
  {

    @Override
    public Class<? extends Command> getCommandClass()
    {
      return null;
    }

    @Override
    public String getCommandName()
    {
      return "testcommand";
    }

    @Override
    public String getDescription()
    {
      return "command for junit tests";
    }
    
  }
  
  @Before
  public void setup()
  {
    sm = System.getSecurityManager();
    output = new ByteArrayOutputStream();
    System.setOut(new PrintStream(output));
  }
  
  @After
  public void teardown()
  {
    System.setSecurityManager(sm);
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testRunNull() throws Exception
  {
    mrgeo.run(null);
        
  }

  @Test()
  @Category(UnitTest.class)
  public void testRunEmpty() throws Exception
  {
    int ret = mrgeo.run(new String[]{});
    Assert.assertEquals("Unexpected return value", -1, ret);
  }
  
  @Test()
  @Category(UnitTest.class)
  public void testHasCommand() throws Exception
  {
    int ret = mrgeo.run(new String[]{});
    Assert.assertEquals("Unexpected return value", -1, ret);
    
    final String stdout = output.toString();
    
    // verify the testcommand is in the list...
    Assert.assertTrue("Missing test command", stdout.contains("\n  testcommand  command for junit tests\n"));

  }

  @Test()
  @Category(UnitTest.class)
  public void testRunInvalidCmd() throws Exception
  {
    int ret = mrgeo.run(new String[]{"foo"});
    Assert.assertEquals("Unexpected return value", -1, ret);
  }

  @Test
  @Category(UnitTest.class)
  public void testMainNoParams()
  {
    System.setSecurityManager(new CaptureExitSecurityManager());
    thrown.expect(ExitException.class);
    thrown.expectMessage("-1");
    MrGeo.main(null);
  }

  @Test
  @Category(UnitTest.class)
  public void testMainHelpParam()
  {
    
    System.setSecurityManager(new CaptureExitSecurityManager());
    thrown.expect(ExitException.class);
    thrown.expectMessage("0");
    MrGeo.main(new String[]{"-h"});
  }

  @Test
  @Category(UnitTest.class)
  public void testMainVerboseParam()
  {
    System.setSecurityManager(new CaptureExitSecurityManager());
    thrown.expect(ExitException.class);
    thrown.expectMessage("-1");
    MrGeo.main(new String[]{"-v"});
  }

  @Test
  @Category(UnitTest.class)
  public void testMainDebugParam()
  {
    System.setSecurityManager(new CaptureExitSecurityManager());
    thrown.expect(ExitException.class);
    thrown.expectMessage("-1");
    MrGeo.main(new String[]{"-d"});
  }

  @Test
  @Category(UnitTest.class)
  public void testMainInvalidCmdParam()
  {
    System.setSecurityManager(new CaptureExitSecurityManager());
    thrown.expect(ExitException.class);
    thrown.expectMessage("-1");
    MrGeo.main(new String[]{"foo"});
  }

}
