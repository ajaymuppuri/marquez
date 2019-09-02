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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;

import java.util.List;
import lombok.NonNull;
import marquez.api.models.JobRunResponse;
import marquez.api.models.JobRunsResponse;
import marquez.service.models.JobRun;

public final class JobRunResponseMapper {
  private JobRunResponseMapper() {}

  public static JobRunResponse map(@NonNull final JobRun run) {
    return JobRunResponse.builder()
        .runId(run.getRunId().toString())
        .createdAt(ISO_INSTANT.format(run.getCreatedAt()))
        .updatedAt(ISO_INSTANT.format(run.getUpdatedAt()))
        .nominalStartTime(
            run.getNominalStartTime()
                .map(nominalStartTime -> ISO_INSTANT.format(nominalStartTime))
                .orElse(null))
        .nominalEndTime(
            run.getNominalEndTime()
                .map(nominalEndTime -> ISO_INSTANT.format(nominalEndTime))
                .orElse(null))
        .runArgs(run.getRunArgs())
        .build();
  }

  public static List<JobRunResponse> map(@NonNull final List<JobRun> runs) {
    return runs.stream().map(run -> map(run)).collect(toImmutableList());
  }

  public static JobRunsResponse toJobRunsResponse(@NonNull final List<JobRun> runs) {
    return JobRunsResponse.builder().runs(map(runs)).build();
  }
}
