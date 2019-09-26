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

import static com.google.common.base.Charsets.UTF_8;

import java.net.URL;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import marquez.common.DatasetName;
import marquez.common.NamespaceName;
import marquez.common.SourceName;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public final class StreamMeta extends DatasetMeta {
  @Getter private final URL schemaLocation;

  public StreamMeta(
      final DatasetName physicalName,
      final SourceName sourceName,
      @NonNull final URL schemaLocation,
      @Nullable final String description,
      @Nullable final UUID runId) {
    super(physicalName, sourceName, description, runId);
    this.schemaLocation = schemaLocation;
  }

  @Override
  public Optional<UUID> version(NamespaceName namespaceName, DatasetName datasetName) {
    final byte[] bytes =
        VERSION_JOINER
            .join(namespaceName.getValue(), datasetName.getValue(), schemaLocation.toString())
            .getBytes(UTF_8);
    final UUID version = UUID.nameUUIDFromBytes(bytes);
    return Optional.of(version);
  }
}
