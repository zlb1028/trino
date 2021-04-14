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

import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.trino.queryeditorui.store.files.ExpiringFileStore;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlUtil;
import io.trino.server.ServerConfig;
import io.trino.spi.security.GroupProvider;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.File;
import java.io.FileInputStream;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Path("/api/files")
public class FilesResource
{
    private final ExpiringFileStore fileStore;
    private final AccessControl accessControl;
    private final ServerConfig serverConfig;
    private final GroupProvider groupProvider;

    @Inject
    public FilesResource(GroupProvider groupProvider, ExpiringFileStore fileStore, AccessControl accessControl, ServerConfig serverConfig)
    {
        this.fileStore = fileStore;
        this.accessControl = accessControl;
        this.serverConfig = serverConfig;
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
    }

    @GET
    @Path("/{fileName}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getFile(@PathParam("fileName") String fileName,
                            @Context HttpHeaders httpHeaders,
                            @Context HttpServletRequest servletRequest)
    {
        // if the user is admin, don't filter results by user.
        Optional<String> filterUser = AccessControlUtil.getUserForFilter(accessControl, serverConfig, groupProvider, httpHeaders, servletRequest);

        final File file = fileStore.get(fileName, filterUser);
        if (file == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        else {
            return Response.ok((StreamingOutput) output ->
            {
                try (FileInputStream inputStream = new FileInputStream(file)) {
                    ByteStreams.copy(inputStream, output);
                }
                finally {
                    output.close();
                }
            }).build();
        }
    }
}
