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

package org.mrgeo.resources.wms;

import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerException;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.GrizzlyTestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.web.GrizzlyWebTestContainerFactory;
import com.sun.jersey.test.framework.spi.container.inmemory.InMemoryTestContainerFactory;
import junit.framework.Assert;
import org.junit.Test;
import org.mrgeo.FilteringInMemoryTestContainerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

public class Tryme extends JerseyTest
{
  @Override
  protected AppDescriptor configure()
  {
    DefaultResourceConfig resourceConfig = new DefaultResourceConfig();
    resourceConfig.getClasses().add(FakeResource.class);
    return new LowLevelAppDescriptor.Builder( resourceConfig ).build();
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException
  {
//    return new FilteringInMemoryTestContainerFactory();
    return new GrizzlyTestContainerFactory();
  }

  @Path("/fake")
  public static class FakeResource
  {
    @Path("/test1")
    @GET
    public Response fake()
    {
      return Response.status(Response.Status.OK).entity("This is the message").build();
    }
  }

  @Test
  public void test1()
  {
    WebResource wr = resource();
    String responseMsg = wr.path("/fake/test1").get(String.class);
    Assert.assertEquals("This is the message", responseMsg);
  }
}
