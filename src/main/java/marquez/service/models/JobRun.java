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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.NonNull;
import lombok.Value;

@Value
public class JobRun {
  @NonNull UUID runId;
  @NonNull Instant createdAt;
  @NonNull Instant updatedAt;
  @Nullable Instant nominalStartTime;
  @Nullable Instant nominalEndTime;
  @NonNull Map<String, String> runArgs;
  @NonNull State runState;

  public Optional<Instant> getNominalStartTime() {
    return Optional.ofNullable(nominalStartTime);
  }

  public Optional<Instant> getNominalEndTime() {
    return Optional.ofNullable(nominalEndTime);
  }

  public enum State {
    NEW,
    RUNNING,
    COMPLETED,
    ABORTED,
    FAILED;
  }
}
