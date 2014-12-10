/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.data.shp.util;

public class RegExTest
{

  /**
   * @param args
   */
  public static void main(String[] args)
  {
    if (args.length != 3)
    {
      System.out.println("USAGE: RegExTest <string> <regex> <replacement>");
      System.exit(1);
    }
    try
    {
      String output = args[0].replaceAll(args[1], args[2]);
      System.out.println("<" + args[0] + "," + args[1] + "," + args[2]);
      System.out.println(">" + output);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

}
