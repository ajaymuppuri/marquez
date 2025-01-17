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

package marquez.service.models;

import java.time.Instant;
import java.util.UUID;
import lombok.Data;
import marquez.common.models.Description;
import marquez.common.models.NamespaceName;
import marquez.common.models.OwnerName;

@Data
public class Namespace {

  private final UUID uuid;
  private final String name;
  private final String owner;
  private final String description;
  private final Instant createdAt;

  public Namespace(UUID uuid, String name, String owner, String description) {
    this.uuid = uuid;
    this.name = name;
    this.owner = owner;
    this.description = description;
    this.createdAt = null;
  }

  public Namespace(UUID uuid, Instant createdAt, String name, String owner, String description) {
    this.uuid = uuid;
    this.createdAt = createdAt;
    this.name = name;
    this.owner = owner;
    this.description = description;
  }

  public static final Namespace DEFAULT =
      new Namespace(
          null,
          null,
          NamespaceName.DEFAULT.getValue(),
          OwnerName.ANONYMOUS.getValue(),
          Description.NO_DESCRIPTION.getValue());
}
