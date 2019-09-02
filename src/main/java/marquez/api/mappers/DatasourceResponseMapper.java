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
import marquez.api.models.DatasourceResponse;
import marquez.api.models.DatasourcesResponse;
import marquez.service.models.Datasource;

public final class DatasourceResponseMapper {
  private DatasourceResponseMapper() {}

  public static DatasourceResponse map(@NonNull final Datasource datasource) {
    return DatasourceResponse.builder()
        .name(datasource.getName().getValue())
        .createdAt(ISO_INSTANT.format(datasource.getCreatedAt()))
        .urn(datasource.getUrn().getValue())
        .connectionUrl(datasource.getConnectionUrl().getRawValue())
        .description(
            datasource.getDescription().map(description -> description.getValue()).orElse(null))
        .build();
  }

  public static List<DatasourceResponse> map(@NonNull final List<Datasource> datasources) {
    return datasources.stream().map(datasource -> map(datasource)).collect(toImmutableList());
  }

  public static DatasourcesResponse toDatasourcesResponse(
      @NonNull final List<Datasource> datasources) {
    return DatasourcesResponse.builder().datasources(map(datasources)).build();
  }
}
