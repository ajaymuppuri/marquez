/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.api.resources;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.List;
import java.util.UUID;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import marquez.api.exceptions.JobNotFoundException;
import marquez.api.exceptions.JobRunNotFoundException;
import marquez.api.exceptions.NamespaceNotFoundException;
import marquez.api.mappers.JobMetaMapper;
import marquez.api.mappers.JobResponseMapper;
import marquez.api.mappers.JobRunMetaMapper;
import marquez.api.mappers.JobRunResponseMapper;
import marquez.api.models.JobRequest;
import marquez.api.models.JobResponse;
import marquez.api.models.JobRunRequest;
import marquez.api.models.JobRunResponse;
import marquez.api.models.JobRunsResponse;
import marquez.api.models.JobsResponse;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.service.JobService;
import marquez.service.NamespaceService;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Job;
import marquez.service.models.JobMeta;
import marquez.service.models.JobRun;
import marquez.service.models.JobRunMeta;

@Path("/api/v1")
public final class JobResource {
  private final JobService jobService;
  private final NamespaceService namespaceService;

  public JobResource(
      @NonNull final NamespaceService namespaceService, @NonNull final JobService jobService) {
    this.namespaceService = namespaceService;
    this.jobService = jobService;
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("/namespaces/{namespace}/jobs/{job}")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response createOrUpdate(
      @PathParam("namespace") String namespaceAsString,
      @PathParam("job") String jobAsString,
      @Valid JobRequest request)
      throws MarquezServiceException {
    final NamespaceName namespaceName = NamespaceName.of(namespaceAsString);
    throwIfNotExists(namespaceName);

    final JobName jobName = JobName.of(jobAsString);
    final JobMeta jobMeta = JobMetaMapper.map(request);
    final Job job = jobService.createOrUpdate(namespaceName, jobName, jobMeta);
    final JobResponse response = JobResponseMapper.map(job);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces/{namespace}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  public Response get(
      @PathParam("namespace") String namespaceAsString, @PathParam("job") String jobAsString)
      throws MarquezServiceException {
    final NamespaceName namespaceName = NamespaceName.of(namespaceAsString);
    throwIfNotExists(namespaceName);

    final JobName jobName = JobName.of(jobAsString);
    final Job job =
        jobService.get(namespaceName, jobName).orElseThrow(() -> new JobNotFoundException(jobName));
    final JobResponse response = JobResponseMapper.map(job);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces/{namespace}/jobs")
  @Produces(APPLICATION_JSON)
  public Response list(
      @PathParam("namespace") String namespaceAsString,
      @QueryParam("limit") @DefaultValue("100") Integer limit,
      @QueryParam("offset") @DefaultValue("0") Integer offset)
      throws MarquezServiceException {
    final NamespaceName namespaceName = NamespaceName.of(namespaceAsString);
    throwIfNotExists(namespaceName);

    final List<Job> jobs = jobService.getAllJobsFor(namespaceName, limit, offset);
    final JobsResponse response = JobResponseMapper.toJobsResponse(jobs);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Path("namespaces/{namespace}/jobs/{job}/runs")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response createRun(
      @PathParam("namespace") String namespaceAsString,
      @PathParam("job") String jobAsString,
      @Valid JobRunRequest request)
      throws MarquezServiceException {
    final NamespaceName namespaceName = NamespaceName.of(namespaceAsString);
    throwIfNotExists(namespaceName);
    final JobName jobName = JobName.of(jobAsString);
    throwIfNotExists(jobName);

    final JobRunMeta runMeta = JobRunMetaMapper.map(request);
    final JobRun run = jobService.createRun(namespaceName, jobName, runMeta);
    final JobRunResponse response = JobRunResponseMapper.map(run);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces/{namespace}/jobs/{job}/runs")
  @Produces(APPLICATION_JSON)
  public Response listRuns(
      @PathParam("namespace") String namespaceAsString,
      @PathParam("job") String jobAsString,
      @QueryParam("limit") @DefaultValue("100") Integer limit,
      @QueryParam("offset") @DefaultValue("0") Integer offset)
      throws MarquezServiceException {
    final NamespaceName namespaceName = NamespaceName.of(namespaceAsString);
    throwIfNotExists(namespaceName);
    final JobName jobName = JobName.of(jobAsString);
    throwIfNotExists(jobName);

    final List<JobRun> runs = jobService.getAllRunsFor(namespaceName, jobName, limit, offset);
    final JobRunsResponse response = JobRunResponseMapper.toJobRunsResponse(runs);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/jobs/runs/{id}")
  @Produces(APPLICATION_JSON)
  public Response getRun(@PathParam("id") UUID runId) throws MarquezServiceException {
    final JobRun run =
        jobService.getRun(runId).orElseThrow(() -> new JobRunNotFoundException(runId));
    final JobRunResponse response = JobRunResponseMapper.map(run);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("/jobs/runs/{id}/run")
  @Produces(APPLICATION_JSON)
  public Response markRunAsRunning(@PathParam("id") UUID runId) throws MarquezServiceException {
    return markRunAs(runId, JobRun.State.RUNNING);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("/jobs/runs/{id}/complete")
  @Produces(APPLICATION_JSON)
  public Response markRunAsCompleted(@PathParam("id") UUID runId) throws MarquezServiceException {
    return markRunAs(runId, JobRun.State.COMPLETED);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("/jobs/runs/{id}/fail")
  @Produces(APPLICATION_JSON)
  public Response markRunAsFailed(@PathParam("id") UUID runId) throws MarquezServiceException {
    return markRunAs(runId, JobRun.State.FAILED);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("/jobs/runs/{id}/abort")
  @Produces(APPLICATION_JSON)
  public Response markRunAsAborted(@PathParam("id") UUID runId) throws MarquezServiceException {
    return markRunAs(runId, JobRun.State.ABORTED);
  }

  private Response markRunAs(UUID runId, JobRun.State runState) throws MarquezServiceException {
    final JobRun run = jobService.markRunAs(runId, runState);
    final JobRunResponse response = JobRunResponseMapper.map(run);
    return Response.ok(response).build();
  }

  private void throwIfNotExists(NamespaceName name) throws MarquezServiceException {
    if (!namespaceService.exists(name)) {
      throw new NamespaceNotFoundException(name);
    }
  }

  private void throwIfNotExists(JobName name) throws MarquezServiceException {
    if (!jobService.exists(name)) {
      throw new JobNotFoundException(name);
    }
  }
}
