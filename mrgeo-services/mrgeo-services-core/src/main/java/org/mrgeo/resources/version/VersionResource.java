/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.resources.version;

import org.mrgeo.services.version.VersionService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

/**
 * @author Steve Ingram
 *         Date: 11/18/13
 */
@Path("/")
public class VersionResource {

    @Context
    VersionService service;

    @GET
    @Produces("application/json")
    public Response getVersionAtApiRoot() {
      return Response.ok().entity(service.getVersionJson()).type("application/json").build();
    }

    @GET
    @Path("/version")
    @Produces("application/json")
    public Response getVersion() {
        return Response.ok().entity( service.getVersionJson() ).type("application/json").build();
    }
}
