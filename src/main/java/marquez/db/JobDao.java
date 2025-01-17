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

import java.util.List;
import java.util.UUID;
import marquez.db.mappers.JobRowMapper;
import marquez.service.models.Job;
import marquez.service.models.JobVersion;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(JobRowMapper.class)
public interface JobDao {
  @CreateSqlObject
  JobVersionDao createJobVersionDao();

  @SqlUpdate(
      "INSERT INTO jobs (uuid, name, namespace_uuid, description, input_dataset_urns, output_dataset_urns, type) "
          + " VALUES (:uuid, :name, :namespaceUuid, :description, :inputDatasetUrns, :outputDatasetUrns, :type)")
  public void insert(@BindBean Job job);

  @SqlUpdate("UPDATE jobs SET current_version_uuid = :currentVersionUuid WHERE uuid = :jobUuid")
  public void setCurrentVersionUuid(UUID jobUuid, UUID currentVersionUuid);

  @Transaction
  default void insertJobAndVersion(final Job job, final JobVersion jobVersion) {
    insert(job);
    createJobVersionDao().insert(jobVersion);
    setCurrentVersionUuid(job.getUuid(), jobVersion.getUuid());
  }

  @SqlQuery(
      "SELECT j.*, jv.uri FROM jobs j INNER JOIN job_versions jv ON (j.uuid = :uuid AND j.current_version_uuid = jv.uuid)")
  Job findByID(UUID uuid);

  @SqlQuery(
      "SELECT j.*, jv.uri "
          + "FROM jobs j "
          + "INNER JOIN job_versions jv "
          + "    ON (j.current_version_uuid = jv.uuid) "
          + "INNER JOIN namespaces n "
          + "    ON (j.namespace_uuid = n.uuid AND n.name = :namespace AND j.name = :name)")
  Job findByName(String namespace, String name);

  @SqlQuery(
      "SELECT j.*, jv.uri "
          + "FROM jobs j "
          + "INNER JOIN job_versions jv "
          + " ON (j.current_version_uuid = jv.uuid) "
          + "INNER JOIN namespaces n "
          + " ON (j.namespace_uuid = n.uuid AND n.name = :namespaceName) "
          + " ORDER BY j.name "
          + "LIMIT :limit OFFSET :offset")
  List<Job> findAllInNamespace(String namespaceName, Integer limit, Integer offset);
}
