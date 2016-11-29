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

package org.mrgeo.resources.job;

import org.glassfish.jersey.test.JerseyTest;
import org.mockito.Mockito;
import org.mrgeo.job.JobTestManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

@SuppressWarnings("all") // Test code, not included in production
public class JobResourceTest extends JerseyTest
{
//    private HttpServletRequest requestMock;
//
//       @Override
//       protected LowLevelAppDescriptor configure() {
//           requestMock = Mockito.mock(HttpServletRequest.class);
//           DefaultResourceConfig resourceConfig = new DefaultResourceConfig();
//           resourceConfig.getClasses().add( JobResource.class );
//           resourceConfig.getSingletons().add( new SingletonTypeInjectableProvider<Context, HttpServletRequest>(HttpServletRequest.class, requestMock){} );
//           return new LowLevelAppDescriptor.Builder( resourceConfig ).build();
//       }
//
//
//       @Override
//       protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
//           return new FilteringInMemoryTestContainerFactory();
//       }

  @Override
  public void setUp() throws Exception
  {
    // TODO Auto-generated method stub
    super.setUp();

//     Mockito.reset(requestMock);
//
//    JobTestManager.getInstance();
//
//    JobManager.getInstance().createJob("job 1", "instructions 1", "type 1");
//    JobManager.getInstance().createJob("job 2 no inst", null, "type 2");
//    JobManager.getInstance().createJob("", "instructions 3 no name", " type 3");
//    long jobid4 = JobManager.getInstance().createJob("job 4 no type 50 failed", "instructions 4", null);
//    long jobid5 = JobManager.getInstance().createJob("job 5 success", "instructions 5", "type 5");
//    try {
//      JobManager.getInstance().updateJobFailed(jobid4, "Job Failed");
//      JobManager.getInstance().updateJobSuccess(jobid5, "result 5", "kml 5");
//    }
//    catch (JobNotFoundException j) {
//
//    }

  }

  @Override
  public void tearDown() throws Exception
  {
    // TODO Auto-generated method stub
    super.tearDown();
//    JobTestManager.reset();
  }

//  @Test
//  @Category(UnitTest.class)
//  public void testGetJobs() throws Exception {
//    Mockito.when(requestMock.getScheme()).thenReturn("http");
//    WebResource webResource = resource();
//    JobsListResponse jobs = webResource.path("job/").get(JobsListResponse.class);
//    Assert.assertEquals(5, jobs.getJobInfo().size());
//
//    JobsListResponse jobs1 = webResource.path("job/")
//        .queryParam("start", "2")
//        .queryParam("pagesize", "2")
//        .get(JobsListResponse.class);
//    Assert.assertEquals(2, jobs1.getJobInfo().size());
//
//    JobsListResponse jobs2 = webResource.path("job/")
//        .queryParam("start", "2")
//        .queryParam("pagesize", "10")
//        .get(JobsListResponse.class);
//    Assert.assertEquals(4, jobs2.getJobInfo().size());
//  }
//
//  @Test(expected=UniformInterfaceException.class)
//  @Category(UnitTest.class)
//  public void testJobNotFound() throws Exception
//  {
//    WebResource webResource = resource();
//    @SuppressWarnings("unused")
//    JobInfoResponse job = webResource.path("job/1000").get(JobInfoResponse.class);
//  }
//
//  @Test
//  @Category(UnitTest.class)
//  public void testGetJob() throws Exception
//  {
//    Mockito.when(requestMock.getScheme()).thenReturn("http");
//    WebResource webResource = resource();
//    JobInfoResponse job = webResource.path("job/4").get(JobInfoResponse.class);
//    Assert.assertEquals(4, job.getJobId());
//    Assert.assertEquals("job 4 no type 50 failed", job.getName());
//    Assert.assertEquals(null, job.getType());
//
//    job = webResource.path("job/5").get(JobInfoResponse.class);
//    Assert.assertEquals(5, job.getJobId());
//    Assert.assertEquals("job 5 success", job.getName());
//    Assert.assertEquals("type 5", job.getType());
//    Assert.assertEquals(100, job.getJobState().getPercent(), 0.1);
//    Assert.assertEquals(JobDetails.COMPLETE, job.getJobState().getState());
//    Assert.assertTrue( job.getStatusUrl().startsWith( "http:" ) );
//  }
//
//  @Test
//  @Category(UnitTest.class)
//  public void testGetJobHttpsHeader() throws Exception
//  {
//      Mockito.when(requestMock.getScheme()).thenReturn("http");
//      Mockito.when(requestMock.getHeader(Mockito.anyString())).thenReturn("on");
//      WebResource webResource = resource();
//      JobInfoResponse job = webResource.path("job/5")
//              .header("X-Forwarded-SSL", "on")
//              .get(JobInfoResponse.class);
//      Assert.assertEquals(5, job.getJobId());
//      Assert.assertEquals("job 5 success", job.getName());
//      Assert.assertEquals("type 5", job.getType());
//      Assert.assertEquals(100, job.getJobState().getPercent(), 0.1);
//      Assert.assertEquals(JobDetails.COMPLETE, job.getJobState().getState());
//      Assert.assertTrue(
//              "URL should start with 'https', was '" + job.getStatusUrl() + "'",
//              job.getStatusUrl().startsWith( "https:" )
//      );
//  }
//
//  @Test
//  @Category(UnitTest.class)
//  public void testGetJobDetailed() throws Exception
//  {
//    Mockito.when(requestMock.getScheme()).thenReturn("http");
//    WebResource webResource = resource();
//    JobInfoDetailedResponse job = webResource.path("job/4/detail").get(JobInfoDetailedResponse.class);
//    Assert.assertEquals(4, job.getJobId());
//    Assert.assertEquals("job 4 no type 50 failed", job.getName());
//    Assert.assertEquals(null, job.getType());
//    Assert.assertEquals("instructions 4", job.getInstructions());
//    Assert.assertEquals(50, job.getJobState().getPercent(), 0.1);
//    Assert.assertEquals(JobDetails.FAILED, job.getJobState().getState());
//    Assert.assertTrue( job.getStatusUrl().startsWith( "http:" ) );
//
//    job = webResource.path("job/2/detail").get(JobInfoDetailedResponse.class);
//    Assert.assertEquals(2, job.getJobId());
//    Assert.assertEquals("job 2 no inst", job.getName());
//    Assert.assertEquals("type 2", job.getType());
//    Assert.assertEquals(null, job.getInstructions());
//
//  }
//
//  @Test
//  @Category(UnitTest.class)
//  public void testGetJobDetailedHttpsHeader() throws Exception
//  {
//      Mockito.when(requestMock.getScheme()).thenReturn("http");
//      Mockito.when(requestMock.getHeader(Mockito.anyString())).thenReturn("on");
//      WebResource webResource = resource();
//      JobInfoDetailedResponse job = webResource.path("job/4/detail")
//              .header("X-Forwarded-SSL", "on")
//              .get(JobInfoDetailedResponse.class);
//      Assert.assertEquals(4, job.getJobId());
//      Assert.assertEquals("job 4 no type 50 failed", job.getName());
//      Assert.assertEquals(null, job.getType());
//      Assert.assertEquals("instructions 4", job.getInstructions());
//      Assert.assertEquals(50, job.getJobState().getPercent(), 0.1);
//      Assert.assertEquals(JobDetails.FAILED, job.getJobState().getState());
//      Assert.assertTrue(
//              "URL should start with 'https', was '" + job.getStatusUrl() + "'",
//              job.getStatusUrl().startsWith( "https:" )
//      );
//  }
}
