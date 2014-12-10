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

package org.mrgeo.data.tsv;

import org.mrgeo.data.csv.CsvInputStream;

import java.io.IOException;
import java.io.InputStream;


/**
 * It is assumed that all TSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
public class TsvInputStream extends CsvInputStream
{

  public TsvInputStream(InputStream is) throws IOException
  {
    super(is, "\t");
  }
  
  public TsvInputStream(String fileName) throws IOException
  {
    super(fileName, "\t");
  }
}
