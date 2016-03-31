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

package org.mrgeo.hdfs.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.mrgeo.hdfs.tile.FileSplit;

/**
 * SequenceFileInputFormat, when given a path to a directory containing a mapfile "data" and
 * "index", should correctly pick out data, and process it, just as it would with sequence files.
 * However, it isn't. In particular, it is also other files like index or splits, and naturally 
 * chokes. As a workaround, we provide this PathFilter, which explicitly excludes index files.
 * Future releases of hadoop may not need this workaround.
 * 
 * This class is package private, since only the input formats use it 
 */
public class MapFileFilter implements PathFilter
{
  @Override
  public boolean accept(final Path path)
  {
    String name = path.getName();
    return !(name.equals("index") ||
        name.equals(FileSplit.SPLIT_FILE) ||
        name.equals(FileSplit.OLD_SPLIT_FILE) ||
        name.equals("_SUCCESS") ||    // these are sometimes created by hadoop
        name.endsWith("$folder$"));   // these are automatically created in S3, yuck!

  }
}
