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

package marquez.api.resources;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static marquez.common.models.Description.NO_DESCRIPTION;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.List;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import marquez.api.exceptions.DatasourceNotFoundException;
import marquez.api.mappers.DatasourceResponseMapper;
import marquez.api.models.DatasourceRequest;
import marquez.api.models.DatasourceResponse;
import marquez.api.models.DatasourcesResponse;
import marquez.common.models.ConnectionUrl;
import marquez.common.models.DatasourceName;
import marquez.common.models.DatasourceUrn;
import marquez.common.models.Description;
import marquez.service.DatasourceService;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Datasource;
import marquez.service.models.DatasourceMeta;

@Path("/api/v1/datasources")
public final class DatasourceResource {
  private final DatasourceService service;

  public DatasourceResource(@NonNull final DatasourceService service) {
    this.service = service;
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response create(@Valid DatasourceRequest request) throws MarquezServiceException {
    final DatasourceMeta meta =
        DatasourceMeta.builder()
            .name(DatasourceName.of(request.getName()))
            .connectionUrl(ConnectionUrl.of(request.getConnectionUrl()))
            .description(request.getDescription().map(Description::of).orElse(NO_DESCRIPTION))
            .build();
    final Datasource datasource = service.createOrUpdate(meta);
    final DatasourceResponse response = DatasourceResponseMapper.map(datasource);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("{urn}")
  public Response get(@PathParam("urn") String urnAsString) throws MarquezServiceException {
    final DatasourceUrn urn = DatasourceUrn.of(urnAsString);
    final DatasourceResponse response =
        service
            .get(urn)
            .map(DatasourceResponseMapper::map)
            .orElseThrow(() -> new DatasourceNotFoundException(urn));
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  public Response list(
      @QueryParam("limit") @DefaultValue("100") Integer limit,
      @QueryParam("offset") @DefaultValue("0") Integer offset)
      throws MarquezServiceException {
    final List<Datasource> datasources = service.getAll(limit, offset);
    final DatasourcesResponse response =
        DatasourceResponseMapper.toDatasourcesResponse(datasources);
    return Response.ok(response).build();
  }
}
