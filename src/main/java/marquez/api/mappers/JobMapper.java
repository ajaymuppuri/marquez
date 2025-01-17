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

import lombok.NonNull;
import marquez.api.models.JobRequest;
import marquez.common.models.JobName;
import marquez.common.models.JobType;
import marquez.service.models.Job;

public final class JobMapper {
  private JobMapper() {}

  public static Job map(@NonNull JobName jobName, @NonNull JobRequest request) {
    return new Job(
        null,
        JobType.valueOf(request.getType()),
        jobName.getValue(),
        request.getLocation(),
        null,
        request.getDescription().orElse(null),
        request.getInputDatasetUrns(),
        request.getOutputDatasetUrns());
  }
}
