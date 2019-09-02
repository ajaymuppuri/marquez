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

package marquez.api.mappers;

import java.time.Instant;
import lombok.NonNull;
import marquez.api.models.JobRunRequest;
import marquez.service.models.JobRunMeta;

public final class JobRunMetaMapper {
  private JobRunMetaMapper() {}

  public static JobRunMeta map(@NonNull final JobRunRequest request) {
    return JobRunMeta.builder()
        .nominalStartTime(
            request
                .getNominalStartTime()
                .map(nominalStartTime -> Instant.parse(nominalStartTime))
                .orElse(null))
        .nominalEndTime(
            request
                .getNominalEndTime()
                .map(nominalEndTime -> Instant.parse(nominalEndTime))
                .orElse(null))
        .runArgs(request.getRunArgs())
        .build();
  }
}
