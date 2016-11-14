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

package org.mrgeo.resources.mrspyramid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import junit.framework.Assert;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.awt.image.DataBuffer;


public class MetadataResourceTest extends JerseyTest
{
private MrsPyramidService service;

@Override
protected Application configure()
{
  service = Mockito.mock(MrsPyramidService.class);

  ResourceConfig config = new ResourceConfig();
  config.register(MetadataResource.class);
  config.register(new AbstractBinder()
  {
    @Override
    protected void configure()
    {
      bind(service).to(MrsPyramidService.class);
    }
  });
  return config;
}


@Override
public void setUp() throws Exception
{
  super.setUp();
  Mockito.reset(service);
}

@Test
@Category(UnitTest.class)
public void testGetMetadataBadPath() throws Exception {
  Mockito.when(service.getMetadata(Mockito.anyString())).thenAnswer(new Answer() {
    public Object answer(InvocationOnMock invocation) {
      throw new NotFoundException("Path does not exist - " + invocation.getArguments()[0]);
    }

  });

  String resultImg = "small-elevation-bad-path";

  Response response = target("metadata/" + resultImg)
      .request().get();

  Assert.assertEquals(404, response.getStatus());
  Assert.assertEquals("Path does not exist - " + resultImg, response.readEntity(String.class));
}


private String getMetaData() {
  return "{\"bounds\":" +
      "{\"s\":-18.373333333333342,\"w\":141.7066666666667,\"n\":-17.52000000000001,\"e\":142.56000000000003},\"maxZoomLevel\":10,\"imageMetadata\":" +
      "[{\"pixelBounds\":null,\"tileBounds\":null,\"image\":null}," +
      "{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":3,\"maxX\":2}," +
      "\"tileBounds\":{\"minY\":0,\"minX\":1,\"maxY\":0,\"maxX\":1}," +
      "\"image\":\"1\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":5,\"maxX\":5},\"tileBounds\":{\"minY\":0,\"minX\":3,\"maxY\":0,\"maxX\":3}," +
      "\"image\":\"2\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":10,\"maxX\":10},\"tileBounds\":{\"minY\":1,\"minX\":7,\"maxY\":1,\"maxX\":7}," +
      "\"image\":\"3\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":20,\"maxX\":20},\"tileBounds\":{\"minY\":3,\"minX\":14,\"maxY\":3,\"maxX\":14}," +
      "\"image\":\"4\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":39,\"maxX\":39},\"tileBounds\":{\"minY\":6,\"minX\":28,\"maxY\":6,\"maxX\":28}," +
      "\"image\":\"5\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":78,\"maxX\":78},\"tileBounds\":{\"minY\":12,\"minX\":57,\"maxY\":12,\"maxX\":57}," +
      "\"image\":\"6\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":155,\"maxX\":156},\"tileBounds\":{\"minY\":25,\"minX\":114,\"maxY\":25,\"maxX\":114}," +
      "\"image\":\"7\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":311,\"maxX\":311},\"tileBounds\":{\"minY\":50,\"minX\":228,\"maxY\":51,\"maxX\":229}," +
      "\"image\":\"8\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":622,\"maxX\":622},\"tileBounds\":{\"minY\":101,\"minX\":457,\"maxY\":103,\"maxX\":458}," +
      "\"image\":\"9\"},{\"pixelBounds\":{\"minY\":0,\"minX\":0,\"maxY\":1243,\"maxX\":1243},\"tileBounds\":{\"minY\":203,\"minX\":915,\"maxY\":206,\"maxX\":917}," +
      "\"image\":\"10\"}]," +
      "\"bands\":1,\"defaultValues\":[-9999.0],\"tilesize\":512,\"tileType\":4}";
}

@Test
@Category(UnitTest.class)
public void testGetMetadata() throws Exception {
  Mockito.when(service.getMetadata(Mockito.anyString())).thenReturn(getMetaData());

  String response = target("metadata/all-ones")
      .request().get(String.class);

  ObjectMapper mapper = new ObjectMapper();
  MrsPyramidMetadata md = mapper.readValue(response, MrsPyramidMetadata.class);
  Assert.assertEquals(1, md.getBands());
  Assert.assertEquals(10, md.getMaxZoomLevel());
  Bounds bounds = md.getBounds();

  Assert.assertEquals("Bad bounds - min x", 141.7066, bounds.w, 0.0001);
  Assert.assertEquals("Bad bounds - min y", -18.3733, bounds.s, 0.0001);
  Assert.assertEquals("Bad bounds - max x", 142.56, bounds.e, 0.0001);
  Assert.assertEquals("Bad bounds - max y", -17.52, bounds.n, 0.0001);

  Assert.assertEquals(MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, md.getTilesize());
  Assert.assertEquals(DataBuffer.TYPE_FLOAT, md.getTileType());

  LongRectangle tb = md.getTileBounds(10);
  Assert.assertEquals("Bad tile bounds - min x", 915, tb.getMinX());
  Assert.assertEquals("Bad tile bounds - min y", 203, tb.getMinY());
  Assert.assertEquals("Bad tile bounds - max x", 917, tb.getMaxX());
  Assert.assertEquals("Bad tile bounds - max y", 206, tb.getMaxY());
}
}
