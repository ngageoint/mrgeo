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

package org.mrgeo.resources.version;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.services.version.VersionService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Providers;

/**
 * @author Steve Ingram
 *         Date: 11/18/13
 */
@Path("/")
public class VersionResource
{

@Context
Providers providers;

@Context
VersionService service;

@GET
@Produces("application/json")
public Response getVersionAtApiRoot()
{
  getService();
  return Response.ok().entity(service.getVersionJson()).type("application/json").build();
}

@SuppressFBWarnings(value = "JAXRS_ENDPOINT", justification = "verified")
@GET
@Path("/version")
@Produces("application/json")
public Response getVersion()
{
  getService();
  return Response.ok().entity(service.getVersionJson()).type("application/json").build();
}

private void getService()
{
  if (service == null)
  {
    ContextResolver<VersionService> resolver =
        providers.getContextResolver(VersionService.class, MediaType.WILDCARD_TYPE);
    service = resolver.getContext(VersionService.class);
  }
}

}
