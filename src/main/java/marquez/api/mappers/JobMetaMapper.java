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

import static marquez.common.models.Description.NO_DESCRIPTION;

import java.net.URI;
import lombok.NonNull;
import marquez.api.models.JobRequest;
import marquez.common.models.DatasetUrn;
import marquez.common.models.Description;
import marquez.common.models.JobType;
import marquez.service.models.JobMeta;

public final class JobMetaMapper {
  private JobMetaMapper() {}

  public static JobMeta map(@NonNull final JobRequest request) {
    return JobMeta.builder()
        .type(JobType.valueOf(request.getType()))
        .inputDatasetUrns(DatasetUrn.toList(request.getInputDatasetUrns()))
        .outputDatasetUrns(DatasetUrn.toList(request.getOutputDatasetUrns()))
        .location(URI.create(request.getLocation()))
        .description(request.getDescription().map(Description::of).orElse(NO_DESCRIPTION))
        .build();
  }
}
