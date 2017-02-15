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

package org.mrgeo.publisher.rest.geoserver;

import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.publisher.MrGeoPublisher;
import org.mrgeo.publisher.MrGeoPublisherException;
import org.mrgeo.publisher.rest.RestClient;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.io.IOException;

/**
 * Created by ericwood on 8/9/16.
 */
public class GeoserverPublisher implements MrGeoPublisher
{

protected static final String WORKSPACES_URL = "workspaces";
protected static final String COVERAGE_STORES_URL = "coveragestores";
protected static final String COVERAGES_URL = "coverages";
protected static final String EPSG = "EPSG:4326";
static final String NATIVE_FORMAT = "GEOTIFF";
private static final Logger logger = LoggerFactory.getLogger(GeoserverPublisher.class);
//
//
//    protected String baseUrl;
//    protected String geoserverUsername;
//    protected String geoserverPassword;
protected String workspace;
protected String coverageStoreName;
protected RestClient restClient;
private String coverageStoreDescription;
private String coverageStoreType;
private String coverageStoreConfigUrl;

public String getWorkspace()
{
  return workspace;
}

public void setWorkspace(String workspace)
{
  this.workspace = workspace;
}

public String getCoverageStoreName()
{
  return coverageStoreName;
}

public void setCoverageStoreName(String coverageStoreName)
{
  this.coverageStoreName = coverageStoreName;
}

public void setGeoserverEndpoint(String baseUrl, String username, String password)
{
  restClient = new RestClient(baseUrl, username, password);
}

public void setCoverageStoreDescription(String coverageStoreDescription)
{
  this.coverageStoreDescription = coverageStoreDescription;
}

public void setCoverageStoreType(String coverageStoreType)
{
  this.coverageStoreType = coverageStoreType;
}

public void setCoverageStoreConfigUrl(String coverageStoreConfigUrl)
{
  this.coverageStoreConfigUrl = coverageStoreConfigUrl;
}

@Override
public void publishImage(String imageName, MrsPyramidMetadata imageMetadata) throws MrGeoPublisherException
{
  publishWorkspace();
  publishCoverageStore();
  publishCoverage(imageName, imageMetadata);

}

protected void publishWorkspace() throws MrGeoPublisherException
{
  // Determine if the workspace exists already
  // Get the workspaces
  String workspacesJson;
  try
  {
    workspacesJson = restClient.getJson(WORKSPACES_URL + ".json");
  }
  catch (IOException e)
  {
    throw new MrGeoPublisherException("Unexpected error retrieving the list of workspaces", e);
  }
  try
  {
    if (!GeoserverJsonUtils.getWorkspaceNames(workspacesJson).contains(workspace))
    {
      restClient.postJson(WORKSPACES_URL, GeoserverJsonUtils.createWorkspace(workspace).toJson());
    }
  }
  catch (IOException e)
  {
    throw new MrGeoPublisherException("Unexpected error creating workspace " + workspace, e);
  }
}

protected void publishCoverageStore() throws MrGeoPublisherException
{
  // Determine if the store exists already
  // Get the stores
  String coverageStoreUrl = String.format("%1$s/%2$s/%3$s", WORKSPACES_URL, workspace, COVERAGE_STORES_URL);
  String coveragesJson;
  try
  {
    coveragesJson = restClient.getJson(coverageStoreUrl + ".json");
  }
  catch (IOException e)
  {
    throw new MrGeoPublisherException("Unexpected error retrieving the list of coverage stores", e);
  }
  try
  {
    if (!GeoserverJsonUtils.getCoverageStoreNames(coveragesJson).contains(coverageStoreName))
    {
      restClient.postJson(coverageStoreUrl, GeoserverJsonUtils.createCoverageStore(coverageStoreName)
          .workspace(workspace)
          .description(coverageStoreDescription)
          .type(coverageStoreType)
          .enabled(true)
          .configUrl(coverageStoreConfigUrl)
          .toJson());
    }
  }
  catch (IOException e)
  {
    throw new MrGeoPublisherException("Unexpected error creating coverage store " + coverageStoreName, e);
  }
}

protected void publishCoverage(String imageName, MrsPyramidMetadata imageMetadata) throws MrGeoPublisherException
{
  String coverageJson = getCoverageJsonForImage(imageName, imageMetadata);
  logger.debug("Coverage - " + coverageJson);
  // Determine if the coverage exists already
  // Get the coverages
  String coveragesUrl = String.format("%1$s/%2$s/%3$s/%4$s/%5$s", WORKSPACES_URL, workspace,
      COVERAGE_STORES_URL, coverageStoreName, COVERAGES_URL);
  String coveragesJson;
  try
  {
    coveragesJson = restClient.getJson(coveragesUrl + ".json");
  }
  catch (IOException e)
  {
    throw new MrGeoPublisherException("Unexpected error retrieving the list of coverages", e);
  }
  try
  {
    if (GeoserverJsonUtils.getCoverageNames(coveragesJson).contains(imageName))
    {
      restClient.putJson(String.format("%1$s/%2$s.json", coveragesUrl, imageName), coverageJson);
    }
    else
    {
      restClient.postJson(coveragesUrl, coverageJson);
    }
  }
  catch (IOException e)
  {
    throw new MrGeoPublisherException("Unexpected error creating coverage store " + coverageStoreName, e);
  }
}

protected String getCoverageJsonForImage(String coverageName, MrsPyramidMetadata imageMetadata)
{
  Bounds bounds = imageMetadata.getBounds();
  int zoomLevel = imageMetadata.getMaxZoomLevel();
  LongRectangle pixelBounds = imageMetadata.getPixelBounds(zoomLevel);
  double resolution = TMSUtils.resolution(zoomLevel, imageMetadata.getTilesize());
  GeoserverJsonUtils.GeoserverCoverageBuilder coverageBuilder = GeoserverJsonUtils.createCoverage(coverageName);
  GeoserverJsonUtils.GeoserverBoundingBoxBuilder boundingBoxBuilder =
      new GeoserverJsonUtils.GeoserverBoundingBoxBuilder();
  coverageBuilder
      .nativeName(coverageName)
      .title(coverageName)
      .description("Created from MrGeo image " + coverageName)
      .nativeCRS(GDALUtils.EPSG4326())
      .srs(EPSG)
      .nativeBoundingBox(boundingBoxBuilder
          .minx(bounds.w)
          .maxx(bounds.e)
          .miny(bounds.s)
          .maxy(bounds.n)
          .crs(EPSG)
          .build())
      // Can reuse boundingBoxBuilder becasue the bounding box is the same
      .latLonBoundingBox(boundingBoxBuilder.build())
      .enabled(true)
      .nativeFormat(NATIVE_FORMAT)
      .grid(new GeoserverJsonUtils.GeoserverGridBuilder()
          .dimension(2)
          .range(new GeoserverJsonUtils.GeoserverGridRangeBuilder()
              .low(0, 0)
              .high(pixelBounds.getMaxX(), pixelBounds.getMaxY())
              .build())
          .transform(new GeoserverJsonUtils.GeoserverTransformBuilder()
              .scaleX(resolution)
              .scaleY(resolution)
              .shearX(0.0)
              .shearY(0.0)
              .translateX(bounds.w)
              .translateY(bounds.n)
              .build())
          .crs(EPSG)
          .build())
      .supportedFormats(new String[]{"GIF", "PNG", "JPEG", "TIFF", "ImageMosaic", "GEOTIFF", "ArcGrid", "Gtopo30"})
      .requestSRS(EPSG)
      .responseSRS(EPSG);

  // Add a dimension for each band
  for (int i = 0; i < imageMetadata.getBands(); i++)
  {
    addCoverageDimension(i, imageMetadata, coverageBuilder);

  }

  return coverageBuilder.toJson();

}

private void addCoverageDimension(int band, MrsPyramidMetadata imageMetadata,
    GeoserverJsonUtils.GeoserverCoverageBuilder coverageBuilder)
{
  ImageStats stats = imageMetadata.getImageStats(imageMetadata.getMaxZoomLevel(), band);
  String dimensionType = null;
  String dimensionDescriptionType = null;

  switch (imageMetadata.getTileType())
  {
  case DataBuffer.TYPE_BYTE:
    dimensionType = "UNSIGNED_8BITS";
    dimensionDescriptionType = "byte";
    break;
  case DataBuffer.TYPE_SHORT:
    dimensionType = "SIGNED_16BITS";
    dimensionDescriptionType = "short";
    break;
  case DataBuffer.TYPE_USHORT:
    dimensionType = "UNSIGNED_16BITS";
    dimensionDescriptionType = "unsigned short";
    break;
  case DataBuffer.TYPE_INT:
    dimensionType = "SIGNED_32BITS";
    dimensionDescriptionType = "int";
    break;
  case DataBuffer.TYPE_FLOAT:
    dimensionType = "REAL_32BITS";
    dimensionDescriptionType = "float";
    break;
  case DataBuffer.TYPE_DOUBLE:
    dimensionType = "REAL_64BITS";
    dimensionDescriptionType = "double";
    break;
  default:
    break;
  }

  // Don't use the cached builder for each dimension
  coverageBuilder.coverageDimension(new GeoserverJsonUtils.GeoserverCoverageDimensionBuilder()
      .name("band " + band)
      .range(stats.min, stats.max)
      .type(dimensionType)
      .description(String.format("Band %1$d (%2$s)", band + 1, dimensionDescriptionType))
      .build());

}

//    public void setGeoserverUsername(String geoserverUsername) {
//        this.geoserverUsername = geoserverUsername;
//    }
//
//    public void setGeoserverPassword(String geoserverPassword) {
//        this.geoserverPassword = geoserverPassword;
//    }

}
