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

import static marquez.common.base.MorePreconditions.checkNotBlank;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public final class JobResponse {
  @Getter private final String type;
  @Getter private final String name;
  @Getter private final String createdAt;
  @Getter private final String updatedAt;
  @Getter private final List<String> inputDatasetUrns;
  @Getter private final List<String> outputDatasetUrns;
  @Getter private final String location;
  private final String description;

  public JobResponse(
      @NonNull final String type,
      @NonNull final String name,
      @NonNull final String createdAt,
      @NonNull final String updatedAt,
      @NonNull final List<String> inputDatasetUrns,
      @NonNull final List<String> outputDatasetUrns,
      @NonNull final String location,
      @Nullable final String description) {
    this.type = checkNotBlank(type);
    this.name = checkNotBlank(name);
    this.createdAt = checkNotBlank(createdAt);
    this.updatedAt = checkNotBlank(updatedAt);
    this.inputDatasetUrns = inputDatasetUrns;
    this.outputDatasetUrns = outputDatasetUrns;
    this.location = checkNotBlank(location);
    this.description = description;
  }

  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }
}
