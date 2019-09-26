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
import static java.util.stream.Collectors.joining;

import com.google.common.base.Joiner;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import marquez.common.DatasetName;
import marquez.common.JobName;
import marquez.common.JobType;
import marquez.common.NamespaceName;

@Value
public class JobMeta {
  private static final String VERSION_DELIM = ":";
  private static final Joiner VERSION_JOINER = Joiner.on(VERSION_DELIM);

  @NonNull JobType type;
  @NonNull List<DatasetName> inputs;
  @NonNull List<DatasetName> outputs;
  @NonNull URL location;
  @Nullable String description;

  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }

  public UUID version(NamespaceName namespaceName, JobName jobName) {
    final byte[] bytes =
        VERSION_JOINER
            .join(
                namespaceName.getValue(),
                jobName.getValue(),
                inputs.stream().map(input -> input.getValue()).collect(joining(VERSION_DELIM)),
                outputs.stream().map(output -> output.getValue()).collect(joining(VERSION_DELIM)),
                location.toString())
            .getBytes(UTF_8);
    return UUID.nameUUIDFromBytes(bytes);
  }
}
