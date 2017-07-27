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

package org.mrgeo.resources;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

public class Tryme extends JerseyTest
{

@Test
public void test1()
{
  String response = target("/fake/test1").request().get(String.class);
  Assert.assertEquals("Bad message", "This is the message", response);
}

@Override
protected Application configure()
{
  ResourceConfig config = new ResourceConfig();
  config.register(FakeResource.class);

  return config;
}

@Path("/fake")
public static class FakeResource
{
  String message = "This is the message";

  @Path("/test1")
  @GET
  public Response fake()
  {
    return Response.status(Response.Status.OK).entity(message).build();
  }
}
}
