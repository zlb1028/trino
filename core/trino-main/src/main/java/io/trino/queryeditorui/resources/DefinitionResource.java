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
package io.trino.queryeditorui.resources;

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.client.Column;
import io.trino.queryeditorui.metadata.ColumnService;
import io.trino.queryeditorui.metadata.PreviewTableService;
import io.trino.queryeditorui.metadata.SchemaService;
import io.trino.queryeditorui.metadata.TableService;
import io.trino.queryeditorui.protocol.TableInfo;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlUtil;
import io.trino.spi.security.GroupProvider;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;

import static io.trino.catalog.DynamicCatalogService.badRequest;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

/**
 * @author lipusheng
 * @date 2021/4/12
 */
@Path("/api/definition")
public class DefinitionResource
{
    private static final JsonCodec<TableInfo> TABLE_INFO_CODEC = JsonCodec.jsonCodec(TableInfo.class);
    private final SchemaService schemaService;
    private final ColumnService columnService;
    private final AccessControl accessControl;
    private final PreviewTableService previewTableService;
    private final GroupProvider groupProvider;
    private final TableService tableService;

    @Inject
    public DefinitionResource(
            final SchemaService schemaService,
            final ColumnService columnService,
            GroupProvider groupProvider,
            final AccessControl accessControl,
            final PreviewTableService previewTableService,
            TableService tableService)
    {
        this.schemaService = schemaService;
        this.columnService = columnService;
        this.accessControl = accessControl;
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.previewTableService = previewTableService;
        this.tableService = tableService;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("schema")
    public Response createSchema(@FormDataParam("catalog") String catalog,
                                 @FormDataParam("schema") String schema,
                                 @FormDataParam("parameters") Map<String, String> parameters,
                                 @Context HttpHeaders httpHeaders,
                                 @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        schemaService.createSchema(catalog, schema, parameters, user);
        return Response.ok().build();
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("schema/{catalog}/{schema}")
    public Response dropSchema(@NotNull @PathParam("catalog") String catalog,
                               @NotNull @PathParam("schema") String schema,
                               @Context HttpHeaders httpHeaders,
                               @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        schemaService.dropSchema(catalog, schema, user);
        return Response.ok().build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("table")
    public Response createTables(@FormDataParam("tableInfo") String tableInfoJson,
                                 @Context HttpHeaders httpHeaders,
                                 @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        tableService.createTable(TABLE_INFO_CODEC.fromJson(tableInfoJson), user);
        return Response.ok().build();
    }

    private TableInfo toTableInfo(String tableInfoJson)
    {
        if (tableInfoJson == null) {
            throw badRequest(BAD_REQUEST, "Catalog information is missing");
        }

        try {
            TableInfo tableInfo = TABLE_INFO_CODEC.fromJson(tableInfoJson);
            return tableInfo;
        }
        catch (IllegalArgumentException ex) {
            throw badRequest(BAD_REQUEST, "Invalid JSON string of catalog information");
        }
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("table/{catalog}/{schema}/{table}")
    public Response deleteTable(@NotNull @PathParam("catalog") String catalog,
                                @NotNull @PathParam("schema") String schema,
                                @NotNull @PathParam("table") String table,
                                 @Context HttpHeaders httpHeaders,
                                 @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        tableService.dropTable(catalog, schema, table, user);
        return Response.ok().build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("column")
    public Response addColumn(@FormDataParam("tableInfo") String tableInfoJson,
                              @FormDataParam("column") Column column,
                              @Context HttpHeaders httpHeaders,
                              @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        columnService.addColumn(toTableInfo(tableInfoJson), column, user);
        return Response.ok().build();
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("column/{catalog}/{schema}/{table}/{column}")
    public Response dropColumn(@NotNull @PathParam("catalog") String catalog,
                               @NotNull @PathParam("schema") String schema,
                               @NotNull @PathParam("table") String table,
                               @FormDataParam("column") String column,
                               @Context HttpHeaders httpHeaders,
                               @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        columnService.dropColumn(catalog, schema, table, column, user);
        return Response.ok().build();
    }
}
