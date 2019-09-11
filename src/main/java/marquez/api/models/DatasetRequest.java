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

package marquez.api.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = DbTableRequest.class, name = "DB_TABLE"),
  @JsonSubTypes.Type(value = StreamRequest.class, name = "STREAM")
})
public abstract class DatasetRequest {
  @Getter private final String type;
  @Getter private final String name;
  @Getter private final String physicalName;
  @Getter private final String datasourceName;
  private final String description;

  public DatasetRequest(
      final String type,
      final String name,
      final String physicalName,
      final String datasourceName,
      @Nullable final String description) {
    this.type = type;
    this.name = name;
    this.physicalName = physicalName;
    this.datasourceName = datasourceName;
    this.description = description;
  }

  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }
}
