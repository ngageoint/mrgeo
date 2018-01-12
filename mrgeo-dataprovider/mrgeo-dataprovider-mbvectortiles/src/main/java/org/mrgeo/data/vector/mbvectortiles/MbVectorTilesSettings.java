/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.vector.mbvectortiles;

import com.almworks.sqlite4java.SQLite;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.ArrayUtils;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.utils.tms.Bounds;

import java.util.Arrays;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Path to sqlite database must be specified by the user")
public class MbVectorTilesSettings
{
  private final String filename;
  private final String[] layers;
  private final int zoom;
  private final int tilesPerPartitions;
  private final Bounds bbox;

  static {
    String sqliteNativePath = MrGeoProperties.getInstance().getProperty(MrGeoConstants.SQLITE_NATIVE_PATH);
    if (sqliteNativePath != null && !sqliteNativePath.isEmpty()) {
      // If the sqlite native path is relative, then make it relative to the
      // MRGEO_COMMON_HOME diretory.
      if (!new java.io.File(sqliteNativePath).isAbsolute()) {
        String mrgeoHome = System.getProperty(MrGeoConstants.MRGEO_COMMON_HOME, System.getenv(MrGeoConstants.MRGEO_COMMON_HOME));
        sqliteNativePath = new java.io.File(mrgeoHome, sqliteNativePath).getAbsolutePath();
      }
      SQLite.setLibraryPath(sqliteNativePath);
    }
  }

  public MbVectorTilesSettings(final String filename,
                               final String[] layers)
  {
    this(filename, layers, -1, 10000, null);
  }

  public MbVectorTilesSettings(final String filename,
                               final String[] layers,
                               final int zoom,
                               final int recordsPerPartition,
                               final Bounds bbox)
  {
    this.filename = filename;
    if (layers != null) {
      this.layers = Arrays.copyOf(layers, layers.length);
    }
    else {
      this.layers = null;
    }
    this.zoom = zoom;
    this.tilesPerPartitions = recordsPerPartition;
    if (bbox != null) {
      this.bbox = bbox.clone();
    }
    else {
      this.bbox = null;
    }
  }

  public String getFilename() {return filename; }

  public String[] getLayers()
  {
    return ArrayUtils.clone(layers);
  }

  public int getZoom() { return zoom; }

  public int getTilesPerPartition() { return tilesPerPartitions; }

  public Bounds getBbox() { return bbox; }
}
