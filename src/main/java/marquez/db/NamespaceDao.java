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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.db.mappers.NamespaceRowMapper;
import marquez.db.models.NamespaceOwnershipRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.OwnerRow;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(NamespaceRowMapper.class)
public interface NamespaceDao {
  @CreateSqlObject
  OwnerDao createOwnerDao();

  @CreateSqlObject
  NamespaceOwnershipDao createNamespaceOwnershipDao();

  @Transaction
  default NamespaceRow insertAndGetWith(
      NamespaceRow namespaceRow, OwnerRow ownerRow, NamespaceOwnershipRow ownershipRow) {
    insert(namespaceRow);
    createOwnerDao().insert(ownerRow);
    createNamespaceOwnershipDao().insert(ownershipRow);
    return findBy(namespaceRow.getUuid()).get();
  }

  @Transaction
  default NamespaceRow updateAndGetWith(NamespaceRow namespaceRow, OwnerRow ownerRow) {
    final NamespaceRow namespaceRowFound = findBy(namespaceRow.getName()).get();
    if (namespaceRowFound.getCurrentOwnerName().equals(namespaceRow.getCurrentOwnerName())) {
      return namespaceRowFound;
    }

    final OwnerRow ownerRowFound =
        createOwnerDao().findBy(namespaceRowFound.getCurrentOwnerName()).get();

    final NamespaceOwnershipRow endOfOwnershipRow = new NamespaceOwnershipRow();
    endOfOwnershipRow.setEndedAt(Instant.now());
    endOfOwnershipRow.setNamespaceUuid(namespaceRowFound.getUuid());
    endOfOwnershipRow.setOwnerUuid(ownerRowFound.getUuid());
    createNamespaceOwnershipDao().update(endOfOwnershipRow);

    createOwnerDao().insert(ownerRow);

    final NamespaceOwnershipRow startOfOwnershipRow = new NamespaceOwnershipRow();
    startOfOwnershipRow.setUuid(UUID.randomUUID());
    startOfOwnershipRow.setStartedAt(Instant.now());
    startOfOwnershipRow.setNamespaceUuid(namespaceRowFound.getUuid());
    startOfOwnershipRow.setOwnerUuid(ownerRow.getUuid());
    createNamespaceOwnershipDao().insert(startOfOwnershipRow);

    update(namespaceRow);

    return findBy(namespaceRow.getName()).get();
  }

  @SqlUpdate(
      "INSERT INTO namespaces (uuid, created_at, updated_at, name, description, current_owner_name) "
          + "VALUES (:uuid, :createdAt, :updatedAt, :name, :description, :currentOwnerName)")
  void insert(@BindBean NamespaceRow row);

  @SqlUpdate(
      "UPDATE namespaces "
          + "SET updated_at = :updatedAt, "
          + "    current_owner_name = :currentOwnerName "
          + "WHERE name = :name")
  void update(@BindBean NamespaceRow row);

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM namespaces WHERE name = :name)")
  boolean exists(String name);

  @SqlQuery("SELECT * FROM namespaces WHERE uuid = :rowUuid")
  Optional<NamespaceRow> findBy(UUID rowUuid);

  @SqlQuery("SELECT * FROM namespaces WHERE name = :name")
  Optional<NamespaceRow> findBy(String name);

  @SqlQuery("SELECT * FROM namespaces ORDER BY name LIMIT :limit OFFSET :offset")
  List<NamespaceRow> findAll(int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM namespaces")
  int count();
}
