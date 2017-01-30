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

package org.mrgeo.resources.tms;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import javax.activation.MimetypesFileTypeMap;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("all") // Test code, not included in production
public class TileMapServiceResourceTest
{

private static final Logger log = LoggerFactory.getLogger(TileMapServiceResourceTest.class);

private TileMapServiceResource tms = new TileMapServiceResource();

@Test
@Category(UnitTest.class)
public void testFormatHash()
{
  int tifHash = "tif".hashCode();
  int pngHash = "png".hashCode();
  int jpgHash = "jpg".hashCode();

  assertEquals(114833, tifHash);
  assertEquals(111145, pngHash);
  assertEquals(105441, jpgHash);
}

@Test
@Category(UnitTest.class)
public void testMimetypesFileTypeMap()
{
  //MimetypesFileTypeMap foo = new MimetypesFileTypeMap();
  String type = new MimetypesFileTypeMap().getContentType("output.png");
  assertEquals("image/png", type);
  type = new MimetypesFileTypeMap().getContentType("output.jpg");
  assertEquals("image/jpeg", type);
  type = new MimetypesFileTypeMap().getContentType("output.tif");
  assertEquals("image/tiff", type);
}

@Test
@Category(UnitTest.class)
public void testNormalizeUrl()
{
  String url = "http://localhost:8080/service";
  assertEquals(url, tms.normalizeUrl(url));

  String endingSlashUrl = "http://localhost:8080/service/";
  assertEquals(url, tms.normalizeUrl(endingSlashUrl));
}

@Test
@Category(UnitTest.class)
public void testRootResourceXml()
{
  final String response =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><Services><TileMapService href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0\" title=\"MrGeo Tile Map Service\" version=\"1.0.0\"/></Services>";


  try
  {
    Document doc = tms.rootResourceXml("http://localhost:8080/mrgeo-services/api/tms/");
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer = tf.newTransformer();
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));
    String output = writer.getBuffer().toString();
    assertEquals(response, output);
  }
  catch (ParserConfigurationException | TransformerException ex)
  {
    log.error("Failed to generate root resource xml", ex);
  }
}

@Test
@Category(UnitTest.class)
public void testMrsPyramidToTileMapServiceXml()
{
  final String response =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><TileMapService services=\"http://localhost:8080/mrgeo-services/api/tms\" version=\"1.0.0\"><Title>Tile Map Service</Title><Abstract>MrGeo MrsPyramid rasters available as TMS</Abstract><TileMaps><TileMap href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistance/global-geodetic\" profile=\"global-geodetic\" srs=\"EPSG:4326\" title=\"CostDistance\"/><TileMap href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/Elevation/global-geodetic\" profile=\"global-geodetic\" srs=\"EPSG:4326\" title=\"Elevation\"/><TileMap href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/HumveeFriction/global-geodetic\" profile=\"global-geodetic\" srs=\"EPSG:4326\" title=\"HumveeFriction\"/><TileMap href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistance/global-mercator\" profile=\"global-mercator\" srs=\"EPSG:3857\" title=\"CostDistance\"/><TileMap href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/Elevation/global-mercator\" profile=\"global-mercator\" srs=\"EPSG:3857\" title=\"Elevation\"/><TileMap href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/HumveeFriction/global-mercator\" profile=\"global-mercator\" srs=\"EPSG:3857\" title=\"HumveeFriction\"/></TileMaps></TileMapService>";
  List<String> names = new ArrayList<>();
  names.add("HumveeFriction");
  names.add("CostDistance");
  names.add("Elevation");

  try
  {
    Document doc = tms.mrsPyramidToTileMapServiceXml("http://localhost:8080/mrgeo-services/api/tms/1.0.0", names);
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer = tf.newTransformer();
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));
    String output = writer.getBuffer().toString();
    assertEquals(response, output);
  }
  catch (ParserConfigurationException | TransformerException | UnsupportedEncodingException ex)
  {
    log.error("Failed to convert metadata to TileMapService xml", ex);
  }
}


@Test
@Category(UnitTest.class)
public void testMrsPyramidMetadataToTileMapXml()
{
  final String raster = "CostDistanceHumveeV2";

  final String response =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><TileMap tilemapservice=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0\" version=\"1.0.0\"><Title>CostDistanceHumveeV2</Title><Abstract/><SRS>EPSG:4326</SRS><BoundingBox maxx=\"72.0\" maxy=\"35.0\" minx=\"68.0\" miny=\"33.0\"/><Origin x=\"-180.0\" y=\"-90.0\"/><TileFormat extension=\"tif\" height=\"512\" mime-type=\"image/tiff\" width=\"512\"/><TileSets profile=\"global-geodetic\"><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/1\" order=\"0\" units-per-pixel=\"0.3515625\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/2\" order=\"1\" units-per-pixel=\"0.17578125\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/3\" order=\"2\" units-per-pixel=\"0.087890625\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/4\" order=\"3\" units-per-pixel=\"0.0439453125\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/5\" order=\"4\" units-per-pixel=\"0.02197265625\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/6\" order=\"5\" units-per-pixel=\"0.010986328125\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/7\" order=\"6\" units-per-pixel=\"0.0054931640625\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/8\" order=\"7\" units-per-pixel=\"0.00274658203125\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/9\" order=\"8\" units-per-pixel=\"0.001373291015625\"/><TileSet href=\"http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/10\" order=\"9\" units-per-pixel=\"6.866455078125E-4\"/></TileSets></TileMap>";
  MrsPyramidMetadata mpm = new MrsPyramidMetadata();
  mpm.setBounds(new Bounds(68, 33, 72, 35));
  mpm.setMaxZoomLevel(10);
  mpm.setTilesize(MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);

  try
  {
    Document doc = tms.mrsPyramidMetadataToTileMapXml(raster, "global-geodetic",
        "http://localhost:8080/mrgeo-services/api/tms/1.0.0/CostDistanceHumveeV2/", mpm);
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer = tf.newTransformer();
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));
    String output = writer.getBuffer().toString();
    assertEquals(response, output);
  }
  catch (ParserConfigurationException | TransformerException ex)
  {
    log.error("Failed to convert metadata to TileMap xml", ex);
  }

}
}

