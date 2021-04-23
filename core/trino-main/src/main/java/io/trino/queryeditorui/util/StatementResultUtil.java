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
package io.trino.queryeditorui.util;

import io.airlift.log.Logger;
import io.trino.client.QueryData;
import io.trino.client.QueryResults;
import io.trino.client.StatementClient;
import io.trino.queryeditorui.execution.QueryClient;
import io.trino.queryeditorui.execution.QueryRunner;
import org.joda.time.Duration;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author lipusheng
 * @date 2021/4/19
 */
public class StatementResultUtil
{
    private static final Logger LOG = Logger.get(StatementResultUtil.class);

    private StatementResultUtil()
    {
    }

    public static Boolean definitionStatement(QueryRunner queryRunner, String statement)
    {
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(120), statement);

        final Boolean[] resultSet = new Boolean[1];
        try {
            queryClient.executeWith(new Function<StatementClient, Void>() {
                @Nullable
                @Override
                public Void apply(StatementClient client)
                {
                    QueryData results = client.currentData();
                    if (results.getData() != null) {
                        for (List<Object> row : results.getData()) {
                            resultSet[0] = (Boolean) row.get(0);
                        }
                    }

                    if (results instanceof QueryResults && ((QueryResults) results).getError() != null) {
                        throw new RuntimeException(((QueryResults) results).getError().getMessage());
                    }
                    return null;
                }
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            LOG.error("Caught timeout loading data", e);
        }
        return resultSet[0];
    }

    public static Set<String> queryStatement(QueryRunner queryRunner, String statement)
    {
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(120), statement);

        final Set<String> resultSet = new HashSet();
        try {
            queryClient.executeWith(new Function<StatementClient, Void>() {
                @Nullable
                @Override
                public Void apply(StatementClient client)
                {
                    QueryData results = client.currentData();
                    if (results.getData() != null) {
                        for (List<Object> row : results.getData()) {
                            resultSet.add((String) row.get(0));
                        }
                    }

                    if (results instanceof QueryResults && ((QueryResults) results).getError() != null) {
                        throw new RuntimeException(((QueryResults) results).getError().getMessage());
                    }
                    return null;
                }
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            LOG.error("Caught timeout loading data", e);
        }
        return resultSet;
    }
}
