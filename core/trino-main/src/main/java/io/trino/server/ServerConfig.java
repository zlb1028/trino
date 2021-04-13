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
package io.trino.server;

import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ServerConfig
{
    private static final String DELIMITER = ",";

    private boolean coordinator = true;
    private boolean includeExceptionInResponse = true;
    private Duration gracePeriod = new Duration(2, MINUTES);
    private boolean queryResultsCompressionEnabled = true;
    private final Set<String> admins = new HashSet<>();

    public boolean isAdmin(String user)
    {
        return admins.contains(user);
    }

    public Set<String> getAdmins()
    {
        return ImmutableSet.copyOf(admins);
    }

    @Config("openlookeng.admins")
    public ServerConfig setAdmins(String adminsString)
    {
        if (adminsString == null || adminsString.length() == 0) {
            return this;
        }
        String[] adminsSplit = adminsString.split(DELIMITER);
        Arrays.stream(adminsSplit).forEach(admin -> admins.add(admin.trim()));
        return this;
    }

    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Config("coordinator")
    public ServerConfig setCoordinator(boolean coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    public boolean isIncludeExceptionInResponse()
    {
        return includeExceptionInResponse;
    }

    @Config("http.include-exception-in-response")
    public ServerConfig setIncludeExceptionInResponse(boolean includeExceptionInResponse)
    {
        this.includeExceptionInResponse = includeExceptionInResponse;
        return this;
    }

    public Duration getGracePeriod()
    {
        return gracePeriod;
    }

    @Config("shutdown.grace-period")
    public ServerConfig setGracePeriod(Duration gracePeriod)
    {
        this.gracePeriod = gracePeriod;
        return this;
    }

    public boolean isQueryResultsCompressionEnabled()
    {
        return queryResultsCompressionEnabled;
    }

    @Config("query-results.compression-enabled")
    public ServerConfig setQueryResultsCompressionEnabled(boolean queryResultsCompressionEnabled)
    {
        this.queryResultsCompressionEnabled = queryResultsCompressionEnabled;
        return this;
    }
}
