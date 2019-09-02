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

import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasetUrn;
import marquez.common.models.NamespaceName;
import marquez.db.DatasetDao;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetRowExtended;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.mappers.DatasetMapper;
import marquez.service.mappers.DatasetRowMapper;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetMeta;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

@Slf4j
public class DatasetService {
  private final DatasetDao dao;

  public DatasetService(@NonNull final DatasetDao dao) {
    this.dao = dao;
  }

  public Dataset createOrUpdate(@NonNull NamespaceName namespaceName, @NonNull DatasetMeta meta)
      throws MarquezServiceException {
    try {
      final DatasetRow row = DatasetRowMapper.map(meta);
      return dao.insertAndGet(namespaceName, row)
          .map(DatasetMapper::map)
          .orElseThrow(MarquezServiceException::new);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to create dataset {} for namespace {}.", meta, namespaceName.getValue(), e);
      throw new MarquezServiceException();
    }
  }

  public Boolean exists(@NonNull DatasetUrn urn) throws MarquezServiceException {
    try {
      return dao.exists(urn);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to check dataset: {}", urn.getValue(), e);
      throw new MarquezServiceException();
    }
  }

  public Optional<Dataset> get(@NonNull DatasetUrn urn) throws MarquezServiceException {
    try {
      return dao.findBy(urn).map(DatasetMapper::map);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to get dataset: {}", urn.getValue(), e.getMessage());
      throw new MarquezServiceException();
    }
  }

  public List<Dataset> getAll(
      @NonNull NamespaceName namespaceName, @NonNull Integer limit, @NonNull Integer offset)
      throws MarquezServiceException {
    try {
      final List<DatasetRowExtended> rows = dao.findAll(namespaceName, limit, offset);
      return DatasetMapper.map(rows);
    } catch (UnableToExecuteStatementException e) {
      log.error(
          "Failed to get datasets for namespace {}: limit={}, offset={}",
          namespaceName.getValue(),
          limit,
          offset,
          e);
      throw new MarquezServiceException();
    }
  }
}
