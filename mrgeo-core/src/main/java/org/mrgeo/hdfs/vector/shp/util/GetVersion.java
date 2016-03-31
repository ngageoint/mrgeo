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

package org.mrgeo.hdfs.vector.shp.util;

public class GetVersion
{
  private static final String DATE = "$Date: 2005-09-15 23:10:16 -0400 (Thu, 15 Sep 2005) $";
  private static final String VERSION = "$Revision: 530 $";

  private static String getDate()
  {
    return DATE.substring("$Date: ".length(), DATE.length() - 2);
  }

  public static String getInfo()
  {
    return "Common Build: " + getVersion() + " (" + getDate() + ")";
  }

  private static String getVersion()
  {
    String temp = VERSION.substring("$Revision: ".length(), VERSION.length() - 2);
    return temp;
    // require STRING return, so that 1.10 or 1.20 don't come back as 1.1 or 1.2
    // when returned as DOUBLEs.
  }

  public static void main(String[] args)
  {
    System.out.println("");
    System.out.println(getInfo());
    System.out.println("");
    System.exit(0);
  }
}
