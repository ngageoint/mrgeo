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

package org.mrgeo.resources.raster.job;


import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

@Path("/job/")
public class JobResource
{

@Context
UriInfo uriInfo;

@Context
HttpServletRequest request;

// TODO: Commented out until we decide to revisit after Spark changes.
@GET
@Produces(MediaType.APPLICATION_JSON)
public JobsListResponse getAllJobs(@QueryParam("pagesize") @DefaultValue("20") String sPageSize,
    @QueryParam("start") @DefaultValue("1") String sStartNdx)
{
//    String jobUri = uriInfo.getAbsolutePath().toString();
//    jobUri = HttpUtil.updateSchemeFromHeaders( jobUri, request );
//    JobDetails[] jobDetails = null;
//    String prevUrl=null;
//    String nextUrl=null;
//    String param="";
//    try
//    {
//      param = "start";
//      int startNdx = Integer.valueOf(sStartNdx);
//      if (startNdx == 0) {
//        throw new IllegalArgumentException("start has to be greater than 0");
//      }
//      param = "pagesize";
//      int pagesize = Integer.valueOf(sPageSize);
//      param = "";
//      int prevStart = startNdx - pagesize;
//      if (prevStart >= 0) {
//        prevUrl = jobUri + "?pagesize=" + pagesize + "&start=" + prevStart;
//      }
//      int nextStart = startNdx + pagesize;
//      int jobCount = JobManager.getInstance().getJobCount();
//      if (nextStart > jobCount - 1) {
//        nextStart = jobCount - 1;
//      }
//      if (nextStart > startNdx) {
//        nextUrl = jobUri + "?pagesize=" + pagesize + "&start=" + nextStart;
//      }
//      jobDetails = JobManager.getInstance().getJobs(pagesize, startNdx);
//      if (jobDetails !=null) {
//        JobsListResponse jr = JobResponseFormatter.createJobsListResponse(jobDetails, jobUri, prevUrl, nextUrl);
//        return jr;
//      }
//    }
//    catch (NumberFormatException e) {
//      throw new WebApplicationException(
//          Response
//            .status(Status.BAD_REQUEST)
//            .entity("Couldn't parse parameter : " + param +  " (" + e.getMessage() + ")")
//            .build());
//    }
//    catch (IllegalArgumentException e) {
//      throw new WebApplicationException(
//          Response
//            .status(Status.BAD_REQUEST)
//            .entity("Illegal argument " + " (" + e.getMessage() + ")")
//            .build());
//    }
//    catch (Exception e) {
//      throw new WebApplicationException(
//          Response.status(Status.INTERNAL_SERVER_ERROR)
//            .entity(e.getMessage())
//            .build());
//    }
  return new JobsListResponse();
}

//  @PUT
//  @Produces(MediaType.APPLICATION_JSON)
//  @Path("/{jobid: [0-9]+}/cancel/")
//  public void cancelJob(@PathParam("jobid") String jobId) {
//    try {
//      JobManager.getInstance().cancelJob(Long.valueOf(jobId));
//    }
//    catch(Exception e)
//    {
//      String msg = e.getMessage();
//      if (msg != null && msg.length() > 0)
//      {
//        throw new NotFoundException("Unable to retrieve job jobid = " + jobId + " " + e.getMessage());
//      }
//      throw new NotFoundException("Unable to retrieve job jobid = " + jobId);
//    }
//  }
//
//  @GET
//  @Produces(MediaType.APPLICATION_JSON)
//  @Path("/{jobid: [0-9]+}/detail/")
//  public JobInfoDetailedResponse getJobDetailed(@PathParam("jobid") String jobId) {
//    JobInfoDetailedResponse jresponse =  null;
//    String jobUri = uriInfo.getBaseUri().toString() + "job/";
//    jobUri = HttpUtil.updateSchemeFromHeaders( jobUri, request );
//    try {
//      JobDetails jdet = JobManager.getInstance().getJob(Long.valueOf(jobId));
//      jresponse = JobResponseFormatter.createJobResponseDetailed(jdet, jobUri);
//    }
//    catch(Exception e)
//    {
//      String msg = e.getMessage();
//      if (msg != null && msg.length() > 0)
//      {
//        throw new NotFoundException("Unable to retrieve job jobid = " + jobId + " " + e.getMessage());
//      }
//      throw new NotFoundException("Unable to retrieve job jobid = " + jobId);
//    }
//    return jresponse;
//  }
//
//  @GET
//  @Path("/{jobid: [0-9]+}")
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response getJob(@PathParam("jobid") String jobId) {
//    JobInfoResponse jresponse =  null;
//    String jobUri = uriInfo.getBaseUri().toString() + "job/";
//    jobUri = HttpUtil.updateSchemeFromHeaders( jobUri, request );
//    try {
//      JobDetails jdet = JobManager.getInstance().getJob(Long.valueOf(jobId));
//      jresponse = JobResponseFormatter.createJobResponse(jdet, jobUri);
//      return Response.status(Status.OK).entity(jresponse).build();
//    }
//    catch(Exception e)
//    {
//      String msg = e.getMessage();
//      if (msg != null && msg.length() > 0)
//      {
//        return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Unable to retrieve job jobid = " + jobId + " " + e.getMessage()).build();
//      }
//      return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Unable to retrieve job jobid = " + jobId).build();
//    }
//  }
}
