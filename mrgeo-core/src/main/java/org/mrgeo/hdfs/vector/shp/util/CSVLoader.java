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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.StringTokenizer;

@SuppressWarnings("unchecked")
public class CSVLoader
{

  @SuppressWarnings("unused")
  public static void main(String[] args)
  {
    args = new String[1];
    args[0] = "";
    new CSVLoader();
  }

  public CSVLoader()
  {
    try
    {
      @SuppressWarnings("rawtypes")
      HashMap map = new HashMap(1);
      BufferedReader in = new BufferedReader(new FileReader("p:/used.csv"));
      BufferedWriter out = new BufferedWriter(new FileWriter("p:/used_out.txt"));

      try
      {
        int i = 1;
        String str;
        while ((str = in.readLine()) != null)
        {
          System.out.println(str);
          StringTokenizer st = new StringTokenizer(str, ",");
          String name = st.nextToken().toLowerCase();
          String hash = st.nextToken().toLowerCase();
          if (map.containsKey(hash))
            throw new Exception("Duplicate Hash! " + hash);
          String field = st.nextToken().toLowerCase();
          map.put(hash, name);

          out.write("theme." + i + ".url=" + name);
          out.newLine();
          out.write("theme." + i + ".hash=" + hash);
          out.newLine();
          out.write("theme." + i + ".key=" + field);
          out.newLine();
          out.newLine();
          i++;

        }
      }
      finally
      {
        in.close();
        out.close();
      }
    }
    catch (Exception e)
    {
    }
  }
}
