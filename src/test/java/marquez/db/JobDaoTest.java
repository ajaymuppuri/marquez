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

package marquez.db;

import static marquez.common.models.CommonModelGenerator.newJobType;
import static marquez.service.models.ServiceModelGenerator.newJobVersion;
import static marquez.service.models.ServiceModelGenerator.newJobWithNameSpaceId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import marquez.db.fixtures.AppWithPostgresRule;
import marquez.service.models.Job;
import marquez.service.models.JobVersion;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class JobDaoTest {

  @ClassRule public static final AppWithPostgresRule APP = new AppWithPostgresRule();

  final JobDao jobDao = APP.onDemand(JobDao.class);
  final UUID nsID = UUID.randomUUID();
  final String nsName = "my_ns";
  Job job = newJobWithNameSpaceId(nsID);
  JobVersion jobVersion = newJobVersion(job);

  @Before
  public void setUp() {
    insertNamespace(nsID, nsName, "Amaranta");
    job = newJobWithNameSpaceId(nsID);
    jobVersion = newJobVersion(job);
  }

  @After
  public void tearDown() {
    APP.getJDBI()
        .useHandle(
            handle -> {
              handle.execute("DELETE FROM job_runs;");
              handle.execute("DELETE FROM job_versions;");
              handle.execute("DELETE FROM jobs;");
              handle.execute("DELETE FROM owners;");
              handle.execute("DELETE FROM namespaces;");
            });
  }

  private void insertNamespace(UUID namespaceId, String name, String owner) {
    APP.getJDBI()
        .useHandle(
            handle -> {
              handle.execute(
                  "INSERT INTO namespaces(uuid, name, current_ownership)" + "VALUES (?, ?, ?);",
                  namespaceId,
                  name,
                  owner);
            });
  }

  private void assertJobFieldsMatch(Job job1, Job job2) {
    assertEquals(job1.getNamespaceUuid(), job2.getNamespaceUuid());
    assertEquals(job1.getUuid(), job2.getUuid());
    assertEquals(job1.getName(), job2.getName());
    assertEquals(job1.getLocation(), job2.getLocation());
    assertEquals(job1.getNamespaceUuid(), job2.getNamespaceUuid());
    assertEquals(job1.getInputDatasetUrns(), job2.getInputDatasetUrns());
    assertEquals(job1.getOutputDatasetUrns(), job2.getOutputDatasetUrns());
  }

  @Test
  public void testFindByID() {
    jobDao.insertJobAndVersion(job, jobVersion);
    Job jobFound = jobDao.findByID(job.getUuid());
    assertNotNull(jobFound);
    assertJobFieldsMatch(job, jobFound);
    assertNull(null, jobDao.findByID(UUID.randomUUID()));
  }

  @Test
  public void testFindByName() {
    jobDao.insertJobAndVersion(job, jobVersion);
    Job jobFound = jobDao.findByName(nsName, job.getName());
    assertNotNull(jobFound);
    assertJobFieldsMatch(job, jobFound);
    assertNull(null, jobDao.findByName(nsName, "nonexistent job"));
  }

  @Test
  public void testInsert() {
    JobVersion jobVersion = newJobVersion(job);
    jobDao.insertJobAndVersion(job, jobVersion);
    Job jobFound = jobDao.findByID(job.getUuid());
    assertNotNull(jobFound);
    assertJobFieldsMatch(job, jobFound);
  }

  @Test
  public void testInsert_DiffNsSameName() {
    UUID newNamespaceId = UUID.randomUUID();
    insertNamespace(newNamespaceId, "newNsForDupTest", "Amaranta");
    jobDao.insert(job);
    Job jobWithDiffNsSameName =
        new Job(
            UUID.randomUUID(),
            newJobType(),
            job.getName(),
            "location",
            newNamespaceId,
            "desc",
            Collections.<String>emptyList(),
            Collections.<String>emptyList());
    jobDao.insert(jobWithDiffNsSameName);
  }

  @Test(expected = UnableToExecuteStatementException.class)
  public void testInsert_SameNsSameName() {
    jobDao.insert(job);
    Job jobWithSameNsSameName =
        new Job(
            UUID.randomUUID(),
            newJobType(),
            job.getName(),
            "location",
            job.getNamespaceUuid(),
            "desc",
            Collections.<String>emptyList(),
            Collections.<String>emptyList());
    jobDao.insert(jobWithSameNsSameName);
  }

  @Test
  public void testInsertJobAndVersion() {
    jobDao.insertJobAndVersion(job, jobVersion);
    Job jobFound = jobDao.findByID(job.getUuid());
    assertNotNull(jobFound);
    assertJobFieldsMatch(job, jobFound);
    assertEquals(job.getLocation(), jobFound.getLocation());
  }

  @Test
  public void testFindAllInNamespace() {
    List<Job> jobs =
        Arrays.asList(
            newJobWithNameSpaceId(nsID), newJobWithNameSpaceId(nsID), newJobWithNameSpaceId(nsID));
    jobs.forEach(
        job -> {
          jobDao.insertJobAndVersion(job, newJobVersion(job));
        });
    List<Job> jobsFound = jobDao.findAllInNamespace(nsName, 10, 0);
    assertEquals(jobs.size(), jobsFound.size());
    assertEquals(0, jobDao.findAllInNamespace("nonexistent", 10, 0).size());
    assertEquals(2, jobDao.findAllInNamespace(nsName, 2, 0).size());
    assertEquals(1, jobDao.findAllInNamespace(nsName, 1, 0).size());
    assertEquals(0, jobDao.findAllInNamespace(nsName, 0, 0).size());
    assertEquals(2, jobDao.findAllInNamespace(nsName, 10, 1).size());
    assertEquals(1, jobDao.findAllInNamespace(nsName, 10, 2).size());
    assertEquals(0, jobDao.findAllInNamespace(nsName, 10, 10).size());
  }

  @Test
  public void testFetchJob_EmptyUrns() {
    UUID jobId = UUID.randomUUID();
    Job jobWithEmptyInputsOutputs =
        new Job(
            jobId,
            newJobType(),
            "job",
            "location",
            nsID,
            "description",
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            null,
            null);
    JobVersion jobVersion = newJobVersion(jobWithEmptyInputsOutputs);
    jobDao.insertJobAndVersion(jobWithEmptyInputsOutputs, jobVersion);
    Job jobFound = jobDao.findByID(jobId);
    assertEquals(0, jobFound.getInputDatasetUrns().size());
    assertEquals(0, jobFound.getOutputDatasetUrns().size());
  }
}
