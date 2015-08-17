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

package org.mrgeo.test;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LoggingUtils;

import java.io.IOException;

public class LocalRunnerTest {

  protected Configuration conf = null;

  @BeforeClass
  public static void initJobJar() throws IOException
  {
    // be default, turn logging to ERROR
    LoggingUtils.setDefaultLogLevel(LoggingUtils.ERROR);
    TestUtils.setJarLocation();
  }

  @Before
  public void initLocalRunner() throws IOException
  {
    conf = HadoopUtils.createConfiguration();
    HadoopUtils.setupLocalRunner(conf);
  }

  protected Configuration getConfiguration()
  {
    if (conf == null)
    {
      conf = HadoopUtils.createConfiguration();
    }
    return conf;
  }

  protected ProviderProperties getProviderProperties()
  {
    return null;
  }

  protected String getProtectionLevel()
  {
    return "";
  }
}
