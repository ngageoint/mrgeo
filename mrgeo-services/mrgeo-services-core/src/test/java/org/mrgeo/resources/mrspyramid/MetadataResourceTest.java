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

package org.mrgeo.resources.mrspyramid;

import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import junit.framework.Assert;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mrgeo.FilteringInMemoryTestContainerFactory;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;

import javax.ws.rs.core.Context;
import java.awt.image.DataBuffer;


public class MetadataResourceTest extends JerseyTest
{
    private MrsPyramidService service;

    @Override
    protected LowLevelAppDescriptor configure()
    {
        DefaultResourceConfig resourceConfig = new DefaultResourceConfig();
        service = Mockito.mock(MrsPyramidService.class);

        resourceConfig.getSingletons().add( new SingletonTypeInjectableProvider<Context, MrsPyramidService>(MrsPyramidService.class, service) {});

        resourceConfig.getClasses().add(MetadataResource.class);
        resourceConfig.getClasses().add(JacksonJsonProvider.class);

        return new LowLevelAppDescriptor.Builder( resourceConfig ).build();
    }

    @Override
    protected TestContainerFactory getTestContainerFactory()
    {
        return new FilteringInMemoryTestContainerFactory();
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
                  StringBuilder builder = new StringBuilder();
                  builder.append("Path does not exist - ").append(invocation.getArguments()[0]);
                  throw new NotFoundException(builder.toString());
              }
          });
    WebResource webResource = resource();
    String resultImg = "small-elevation-bad-path";
    ClientResponse res = webResource.path("metadata/" + resultImg).get(ClientResponse.class);
    Assert.assertEquals(404, res.getStatus());
    Assert.assertEquals("Path does not exist - " + resultImg, res.getEntity(String.class));
  }

  private String getMetaData() {
      return "{\"bounds\":" +
      "{\"minY\":-18.373333333333342,\"minX\":141.7066666666667,\"maxY\":-17.52000000000001,\"maxX\":142.56000000000003},\"maxZoomLevel\":10,\"imageMetadata\":" +
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
    WebResource webResource = resource();
    String content = webResource.path("metadata/all-ones" ).get(String.class);
    ObjectMapper mapper = new ObjectMapper();
    MrsPyramidMetadata md = mapper.readValue(content, MrsPyramidMetadata.class);
    Assert.assertEquals(1, md.getBands());
    Assert.assertEquals(10, md.getMaxZoomLevel());
    Bounds bounds = md.getBounds();

    Assert.assertEquals("Bad bounds - min x", 141.7066, bounds.getMinX(), 0.0001);
    Assert.assertEquals("Bad bounds - min y", -18.3733, bounds.getMinY(), 0.0001);
    Assert.assertEquals("Bad bounds - max x", 142.56, bounds.getMaxX(), 0.0001);
    Assert.assertEquals("Bad bounds - max y", -17.52, bounds.getMaxY(), 0.0001);

    Assert.assertEquals(MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, md.getTilesize());
    Assert.assertEquals(DataBuffer.TYPE_FLOAT, md.getTileType());

    LongRectangle tb = md.getTileBounds(10);
    Assert.assertEquals("Bad tile bounds - min x", 915, tb.getMinX());
    Assert.assertEquals("Bad tile bounds - min y", 203, tb.getMinY());
    Assert.assertEquals("Bad tile bounds - max x", 917, tb.getMaxX());
    Assert.assertEquals("Bad tile bounds - max y", 206, tb.getMaxY());
  }
}
