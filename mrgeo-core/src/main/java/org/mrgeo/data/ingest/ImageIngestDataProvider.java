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

package org.mrgeo.data.ingest;

import org.mrgeo.data.tile.MrsTileWriter;

import java.awt.image.Raster;
import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract class to be extended by a data plugin that wishes to provide data
 * for ingesting imagery into MrGeo. A plugin can return null from these methods
 * in cases where it does not support that functionality. See javadocs for the
 * individual methods for more information.
 * 
 * Ingesting imagery into MrGeo involves generating a MrsImage into the
 * back-end data store. It uses a map/reduce job within Hadoop to accomplish
 * that. When the raw image already resides in the back-end data store, the
 * map/reduce job can use it directly as its input. However, if it is a local
 * file, it cannot be used as input for map/reduce because the file would not
 * be accessible on all nodes in the cluster.
 * 
 * In the case where the file is on the local file system, the ingest processing
 * will directly read that image using the InputStream returned from openImage(),
 * tile it, and store those tiles to an intermediate output in the back-end
 * data store. That intermediate tiled data is then used as input for the
 * map/reduce job that generates the MrsImage output, and the getTiledInputFormat()
 * method is used for setting up the input for that map/reduce job.
 */
public abstract class ImageIngestDataProvider extends IngestDataProvider<Raster>
{
  private String resourceName;

  /**
   * This constructor should only be used by sub-classes in the case where
   * they don't know the resource name when they are constructed. They are
   * responsible for subsequently calling setResourceName.
   */
  protected ImageIngestDataProvider()
  {
    resourceName = null;
  }

  /**
   * Sub-classes which use the default constructor must subsequently call
   * this method to assign the resource name,
   * 
   * @param resourceName
   */
  protected void setResourceName(String resourceName)
  {
    this.resourceName = resourceName;
  }

  /**
   * Construct a new instance and associate the resource name passed in with
   * that instance.
   * 
   * @param resourceName
   */
  public ImageIngestDataProvider(String resourceName)
  {
    this.resourceName = resourceName;
  }

  /**
   * Give back the name of the resource this class was originally constructed with.
   * 
   * @return
   */
  public String getResourceName()
  {
    return resourceName;
  }

  /**
   * Give back an input stream to the resource that this class was constructed with.
   * All providers must return a non-null InputStream or else throw an IOException.
   * This method is used when ingesting an image from a local file. If the plugin
   * developer is not interested in supporting that ingest scenario, then it should
   * return null from this method as well as from getTiledInputFormat() and
   * getMrsTileWriter().
   * 
   * @return
   * @throws IOException
   */
  public abstract InputStream openImage() throws IOException;

  /**
   * Give back an input format provider for tiled raw image data (not MrsImage).
   * This provider is used while ingesting an image from the local disk into MrGeo.
   * If a plugin developer is not interested in supporting ingest from a local file
   * then return null from this method. In that case, openImage() should also
   * return null.
   * 
   * @return
   */
  public abstract ImageIngestTiledInputFormatProvider getTiledInputFormat();

  /**
   * Give back a provider for accessing a raw image file (e.g. GeoTiff,
   * jpg, etc...) that resides in the back-end data store. That image will be
   * used as input to a Hadoop map/reduce job for generating a MrsImage. The
   * provider returned from this method is used by the ingest processing for
   * setting up that map/reduce job.
   * 
   * @return
   */
  public abstract ImageIngestRawInputFormatProvider getRawInputFormat();

  /**
   * Give back a tile writer for storing image tiles to intermediate tiled format.
   * This method is used when ingesting imagery from local files. The data written
   * using the returned tile writer is then used as input to a map/reduce job for
   * writing a MrsImage.
   * 
   * @param context
   * @return
   * @throws IOException
   */
  public abstract MrsTileWriter<Raster> getMrsTileWriter(ImageIngestWriterContext context) throws IOException;

  /**
   * Delete this image ingest resource. If there is any problem while performing
   * the delete, an IOException should be thrown.
   * 
   * @throws IOException
   */
  public abstract void delete() throws IOException;

  /**
   * Move this image ingest resource to the specified resource location. If there is
   * any problem while performing the move, an IOException should be thrown.
   * 
   * @param toResource
   * @throws IOException
   */
  public abstract void move(String toResource) throws IOException;
}
