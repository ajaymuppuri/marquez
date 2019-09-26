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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import marquez.common.DatasetName;
import marquez.common.SourceName;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public final class HttpEndpoint extends Dataset {
  public HttpEndpoint(
      final DatasetName name,
      final DatasetName physicalName,
      final Instant createdAt,
      final Instant updatedAt,
      final SourceName sourceName,
      @Nullable final String description) {
    super(name, physicalName, createdAt, updatedAt, sourceName, description);
  }
}
