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

package org.mrgeo.resources.mrspyramid;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Providers;


@Path("/metadata")
public class MetadataResource
{
private static final Logger log = LoggerFactory.getLogger(MetadataResource.class);

@Context
Providers providers;
@Context
MrsPyramidService service;

@SuppressFBWarnings(value = "JAXRS_ENDPOINT", justification = "verified")
@GET
@Produces(MediaType.APPLICATION_JSON)
@Path("/{output: .*+}")
public Response getMetadata(@PathParam("output") final String imgName)
{
  try
  {
    getService();
    return Response.status(Status.OK).entity(service.getMetadata(imgName)).build();
  }
  catch (final NotFoundException e)
  {
    log.error("Exception thrown", e);
    final String error = e.getMessage() != null ? e.getMessage() : "";
    return Response.status(Status.NOT_FOUND).entity(error).build();
  }
  catch (Exception e1)
  {
    log.error("Exception thrown", e1);

    final String error = e1.getMessage() != null ? e1.getMessage() : "";
    return Response.serverError().entity(error).build();
  }
}

private void getService()
{
  if (service == null)
  {
    ContextResolver<MrsPyramidService> resolver =
        providers.getContextResolver(MrsPyramidService.class, MediaType.WILDCARD_TYPE);
    if (resolver != null)
    {
      service = resolver.getContext(MrsPyramidService.class);
    }
  }
}

}
