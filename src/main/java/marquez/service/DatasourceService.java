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

package marquez.service;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasourceUrn;
import marquez.db.DatasourceDao;
import marquez.db.models.DatasourceRow;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.mappers.DatasourceMapper;
import marquez.service.mappers.DatasourceRowMapper;
import marquez.service.models.Datasource;
import marquez.service.models.DatasourceMeta;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

@Slf4j
public class DatasourceService {
  private final DatasourceDao dao;

  public DatasourceService(@NonNull final DatasourceDao dao) {
    this.dao = dao;
  }

  public Datasource createOrUpdate(@NonNull DatasourceMeta meta) throws MarquezServiceException {
    try {
      final DatasourceRow row = DatasourceRowMapper.map(meta);
      return dao.insertAndGet(row)
          .map(DatasourceMapper::map)
          .orElseThrow(MarquezServiceException::new);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to create or update datasource: {}", meta, e);
      throw new MarquezServiceException();
    }
  }

  public Boolean exists(@NonNull DatasourceUrn urn) throws MarquezServiceException {
    try {
      return dao.exists(urn);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to check for datasource: {}", urn.getValue(), e);
      throw new MarquezServiceException();
    }
  }

  public Optional<Datasource> get(@NonNull DatasourceUrn urn) throws MarquezServiceException {
    try {
      return dao.findBy(urn).map(DatasourceMapper::map);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to get datasource: {}", urn.getValue(), e);
      throw new MarquezServiceException();
    }
  }

  public List<Datasource> getAll(@NonNull Integer limit, @NonNull Integer offset)
      throws MarquezServiceException {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    try {
      final List<DatasourceRow> rows = dao.findAll(limit, offset);
      return DatasourceMapper.map(rows);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to get datasources: limit={}, offset={}", limit, offset, e);
      throw new MarquezServiceException();
    }
  }
}
