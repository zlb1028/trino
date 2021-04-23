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
package io.trino.queryeditorui.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.StatementClient;
import io.trino.queryeditorui.QueryEditorUIModule;
import io.trino.queryeditorui.execution.QueryClient;
import io.trino.queryeditorui.execution.QueryRunner;
import io.trino.queryeditorui.execution.QueryRunner.QueryRunnerFactory;
import io.trino.queryeditorui.protocol.ColumnInfo;
import io.trino.queryeditorui.protocol.TableInfo;
import io.trino.queryeditorui.util.StatementResultUtil;
import io.trino.server.protocol.Query;
import io.trino.spi.type.TypeSignature;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnService
{
    private static final Logger log = Logger.get(ColumnService.class);
    private static final Joiner FQN_JOINER = Joiner.on('.').skipNulls();
    private final QueryRunnerFactory queryRunnerFactory;

    @Inject
    public ColumnService(final QueryRunnerFactory queryRunnerFactory)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory session was null!");
    }

    public List<Column> getColumns(String catalogName, String schemaName,
            String tableName, String user) throws ExecutionException
    {
        return queryColumns(FQN_JOINER.join(catalogName, schemaName, tableName), user);
    }

    public void addColumn(TableInfo table, ColumnInfo columnInfo, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String catalogName = table.getCatalog();
        String schemaName = table.getSchema();
        String tableName = table.getTable();
        String statement = String.format("ALTER TABLE %s.%s.%s ADD COLUMN %s %s",
                table.getCatalog(), table.getSchema(), table.getTable(), columnInfo.getName(), columnInfo.getType());
        Boolean result = StatementResultUtil.definitionStatement(queryRunner, statement);
        if (result == null || !result) {
            throw new IllegalArgumentException(String.format("failed to add column [%s: %s] for table [%s.%s]",
                    columnInfo.getName(), columnInfo.getType(), catalogName, schemaName, tableName));
        }
    }

    public void dropColumn(TableInfo table, String column, String user)
    {
        String catalogName = table.getCatalog();
        String schemaName = table.getSchema();
        String tableName = table.getTable();
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = String.format("ALTER TABLE %s.%s.%s DROP COLUMN %s", catalogName, schemaName, tableName, column);
        Boolean result = StatementResultUtil.definitionStatement(queryRunner, statement);
        if (result == null || !result) {
            throw new IllegalArgumentException(String.format("failed to delete column [%s: %s] for table [%s.%s]",
                    column, catalogName, schemaName, tableName, column));
        }
    }

    private List<Column> queryColumns(String fqnTableName, String user)
    {
        String statement = format("SHOW COLUMNS FROM %s", fqnTableName);
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(60), statement);

        final ImmutableList.Builder<Column> cache = ImmutableList.builder();
        try {
            queryClient.executeWith(new Function<StatementClient, Void>() {
                @Nullable
                @Override
                public Void apply(StatementClient client)
                {
                    QueryData results = client.currentData();
                    if (results.getData() != null) {
                        for (List<Object> row : results.getData()) {
                            TypeSignature typeSignature = TypeSignature.parseTypeSignature((String) row.get(1));
                            Column column = new Column((String) row.get(0), (String) row.get(1), Query.toClientTypeSignature(typeSignature));
                            cache.add(column);
                        }
                    }

                    return null;
                }
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading columns", e);
        }

        return cache.build();
    }
}
