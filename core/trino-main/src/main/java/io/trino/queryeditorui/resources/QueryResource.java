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

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.trino.queryeditorui.protocol.Job;
import io.trino.queryeditorui.protocol.Table;
import io.trino.queryeditorui.protocol.queries.CreateSavedQueryBuilder;
import io.trino.queryeditorui.protocol.queries.SavedQuery;
import io.trino.queryeditorui.protocol.queries.UserSavedQuery;
import io.trino.queryeditorui.store.history.JobHistoryStore;
import io.trino.queryeditorui.store.jobs.jobs.ActiveJobsStore;
import io.trino.queryeditorui.store.queries.QueryStore;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlUtil;
import io.trino.server.ServerConfig;
import io.trino.spi.security.GroupProvider;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@Path("/api/query")
public class QueryResource
{
    private final JobHistoryStore jobHistoryStore;
    private final ActiveJobsStore activeJobsStore;
    private final QueryStore queryStore;
    private final AccessControl accessControl;
    private final ServerConfig serverConfig;
    private final GroupProvider groupProvider;

    @Inject
    public QueryResource(GroupProvider groupProvider,
                         JobHistoryStore jobHistoryStore,
                         QueryStore queryStore,
                         ActiveJobsStore activeJobsStore,
                         AccessControl accessControl,
                         ServerConfig serverConfig)
    {
        this.jobHistoryStore = jobHistoryStore;
        this.queryStore = queryStore;
        this.activeJobsStore = activeJobsStore;
        this.accessControl = accessControl;
        this.serverConfig = serverConfig;
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
    }

    public static final Function<Job, DateTime> JOB_ORDERING = (input) ->
    {
        if (input == null) {
            return null;
        }
        return input.getQueryFinished();
    };

    @GET
    @Path("history")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHistory(@QueryParam("table") List<Table> tables,
                               @Context HttpHeaders httpHeaders,
                               @Context HttpServletRequest servletRequest)
    {
        // if the user is admin, don't filter results by user.
        Optional<String> filterUser = AccessControlUtil.getUserForFilter(accessControl, serverConfig, groupProvider, httpHeaders, servletRequest);

        Set<Job> recentlyRun;
        if (tables.size() < 1) {
            recentlyRun = new HashSet<>(jobHistoryStore.getRecentlyRunForUser(filterUser, 200));
            Set<Job> activeJobs = activeJobsStore.getJobsForUser(filterUser);
            recentlyRun.addAll(activeJobs);
        }
        else {
            Table[] tablesArray = tables.toArray(new Table[tables.size()]);
            Table[] restTables = Arrays.copyOfRange(tablesArray, 1, tablesArray.length);
            recentlyRun = new HashSet<>(jobHistoryStore.getRecentlyRunForUser(filterUser, 200, tablesArray[0], restTables));
        }

        List<Job> sortedResult = Ordering
                .natural()
                .nullsLast()
                .onResultOf(JOB_ORDERING)
                .immutableSortedCopy(recentlyRun);
        return Response.ok(sortedResult).build();
    }

    @GET
    @Path("saved")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSaved(@Context HttpHeaders httpHeaders,
                             @Context HttpServletRequest servletRequest)
    {
        // if the user is admin, don't filter results by user.
        Optional<String> filterUser = AccessControlUtil.getUserForFilter(accessControl, serverConfig, groupProvider, httpHeaders, servletRequest);

        return Response.ok(queryStore.getSavedQueries(filterUser)).build();
    }

    @POST
    @Path("saved")
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveQuery(
            @FormParam("description") String description,
            @FormParam("name") String name,
            @FormParam("query") String query,
            @Context HttpHeaders httpHeaders,
            @Context HttpServletRequest servletRequest) throws IOException
    {
        String user = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        CreateSavedQueryBuilder createFeaturedQueryRequest = CreateSavedQueryBuilder.notFeatured()
                .description(description)
                .name(name)
                .query(query);
        if (user != null) {
            SavedQuery savedQuery = createFeaturedQueryRequest.user(user)
                    .build();

            if (queryStore.saveQuery((UserSavedQuery) savedQuery)) {
                return Response.ok(savedQuery.getUuid()).build();
            }
            else {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
        }

        return Response.status(Response.Status.UNAUTHORIZED).build();
    }
}
