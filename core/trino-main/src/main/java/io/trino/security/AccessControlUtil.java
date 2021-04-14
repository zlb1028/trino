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
package io.trino.security;

import com.google.common.collect.ImmutableSet;
import io.trino.SessionRepresentation;
import io.trino.server.BasicQueryInfo;
import io.trino.server.HttpRequestSessionContext;
import io.trino.server.ServerConfig;
import io.trino.server.SessionContext;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;

public final class AccessControlUtil
{
    private AccessControlUtil() {}

    public static void checkCanViewQueryOwnedBy(Identity identity, String queryOwner, AccessControl accessControl)
    {
        if (identity.getUser().equals(queryOwner)) {
            return;
        }
        accessControl.checkCanViewQueryOwnedBy(identity, queryOwner);
    }

    public static List<BasicQueryInfo> filterQueries(Identity identity, List<BasicQueryInfo> queries, AccessControl accessControl)
    {
        String currentUser = identity.getUser();
        Set<String> owners = queries.stream()
                .map(BasicQueryInfo::getSession)
                .map(SessionRepresentation::getUser)
                .filter(owner -> !owner.equals(currentUser))
                .collect(toImmutableSet());
        owners = accessControl.filterQueriesOwnedBy(identity, owners);

        Set<String> allowedOwners = ImmutableSet.<String>builder()
                .add(currentUser)
                .addAll(owners)
                .build();
        return queries.stream()
                .filter(queryInfo -> allowedOwners.contains(queryInfo.getSession().getUser()))
                .collect(toImmutableList());
    }

    public static void checkCanKillQueryOwnedBy(Identity identity, String queryOwner, AccessControl accessControl)
    {
        if (identity.getUser().equals(queryOwner)) {
            return;
        }
        accessControl.checkCanKillQueryOwnedBy(identity, queryOwner);
    }

    public static Optional<String> getUserForFilter(AccessControl accessControl,
                                                    ServerConfig serverConfig,
                                                    GroupProvider groupProvider,
                                                    HttpHeaders httpHeaders,
                                                    HttpServletRequest servletRequest)
    {
        String sessionUser = AccessControlUtil.getUser(accessControl, groupProvider, httpHeaders, servletRequest);
        Optional<String> user = Optional.of(sessionUser);
        // if the user is admin, don't filter results by user.
        if (serverConfig.isAdmin(sessionUser)) {
            user = Optional.empty();
        }
        return user;
    }

    public static String getUser(AccessControl accessControl, GroupProvider groupProvider, HttpHeaders httpHeaders, HttpServletRequest servletRequest)
    {
        String remoteAddress = servletRequest.getRemoteAddr();
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));
        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();
        HttpRequestSessionContext sessionContext = new HttpRequestSessionContext(headers, Optional.of(""), remoteAddress, identity, groupProvider);
        checkCanImpersonateUser(accessControl, sessionContext);
        return sessionContext.getIdentity().getUser();
    }

    public static void checkCanImpersonateUser(AccessControl accessControl, SessionContext sessionContext)
    {
        Identity identity = sessionContext.getIdentity();
        // authenticated identity is not present for HTTP or authentication is not setup
        sessionContext.getAuthenticatedIdentity().ifPresent(authenticatedIdentity -> {
            // only check impersonation is authenticated user is not the same as the explicitly set user
            if (!authenticatedIdentity.getUser().equals(identity.getUser())) {
                accessControl.checkCanImpersonateUser(authenticatedIdentity, identity.getUser());
            }
        });
    }
}
