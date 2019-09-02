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

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.List;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import marquez.api.exceptions.NamespaceNotFoundException;
import marquez.api.mappers.NamespaceMetaMapper;
import marquez.api.mappers.NamespaceResponseMapper;
import marquez.api.models.NamespaceRequest;
import marquez.api.models.NamespaceResponse;
import marquez.api.models.NamespacesResponse;
import marquez.common.models.NamespaceName;
import marquez.service.NamespaceService;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Namespace;
import marquez.service.models.NamespaceMeta;

@Path("/api/v1/namespaces")
public final class NamespaceResource {
  private final NamespaceService service;

  public NamespaceResource(@NonNull final NamespaceService service) {
    this.service = service;
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("{namespace}")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response createOrUpdate(
      @PathParam("namespace") String nameAsString, @Valid NamespaceRequest request)
      throws MarquezServiceException {
    final NamespaceName name = NamespaceName.of(nameAsString);
    final NamespaceMeta meta = NamespaceMetaMapper.map(request);
    final Namespace namespace = service.createOrUpdate(name, meta);
    final NamespaceResponse response = NamespaceResponseMapper.map(namespace);
    return Response.ok(response).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("{namespace}")
  @Produces(APPLICATION_JSON)
  public Response get(@PathParam("namespace") String nameAsString) throws MarquezServiceException {
    final NamespaceName name = NamespaceName.of(nameAsString);
    final Namespace namespace =
        service.get(name).orElseThrow(() -> new NamespaceNotFoundException(name));
    final NamespaceResponse response = NamespaceResponseMapper.map(namespace);
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
    final List<Namespace> namespaces = service.getAll(limit, offset);
    final NamespacesResponse response = NamespaceResponseMapper.toNamespacesResponse(namespaces);
    return Response.ok(response).build();
  }
}
