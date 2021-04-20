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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.queryeditorui.QueryEditorUIModule;
import io.trino.queryeditorui.execution.QueryRunner;
import io.trino.queryeditorui.protocol.CatalogSchema;
import io.trino.queryeditorui.protocol.Table;
import io.trino.queryeditorui.util.StatementResultUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SchemaService
{
    private static final Logger LOG = Logger.get(SchemaService.class);

    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;

    @Inject
    public SchemaService(final QueryRunner.QueryRunnerFactory queryRunnerFactory)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory session was null!");
    }

    public ImmutableList<Table> queryTables(String catalogName, String schemaName, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = format("SHOW TABLES FROM %s.%s", catalogName, schemaName);

        Set<String> tablesResult = StatementResultUtil.queryStatement(queryRunner, statement);

        final ImmutableList.Builder<Table> builder = ImmutableList.builder();
        for (String tableName : tablesResult) {
            builder.add(new Table(catalogName, schemaName, tableName));
        }
        return builder.build();
    }

    public CatalogSchema querySchemas(String catalogName, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = format("SHOW SCHEMAS FROM %s", catalogName);

        Set<String> schemasResult = StatementResultUtil.queryStatement(queryRunner, statement);
        return new CatalogSchema(catalogName, ImmutableList.copyOf(schemasResult));
    }

    public ImmutableList<CatalogSchema> querySchemas(String user)
    {
        Set<String> catalogs = queryCatalogs(user);

        final ImmutableList.Builder<CatalogSchema> builder = ImmutableList.builder();
        for (String catalogName : catalogs) {
            builder.add(querySchemas(catalogName, user));
        }
        return builder.build();
    }

    public Set<String> queryCatalogs(String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = "SHOW CATALOGS";

        return StatementResultUtil.queryStatement(queryRunner, statement);
    }

    public void createSchema(String catalogName, String databaseName, Map<String, String> parameters, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = String.format("CREATE SCHEMA IF NOT EXISTS %s.%s", catalogName, databaseName);
        if (parameters != null && !parameters.isEmpty()) {
            List<String> parametersList = parameters.entrySet().stream()
                    .map(entry -> (String.format("%s = %s", entry.getKey(), entry.getValue()))).collect(Collectors.toList());
            String propertiesStr = parametersList.stream().collect(Collectors.joining(","));
            statement = String.format("CREATE SCHEMA IF NOT EXISTS %s.%s WITH (%s)", catalogName, databaseName, propertiesStr);
        }
        Boolean result = StatementResultUtil.definitionStatement(queryRunner, statement);
        if (result == null || !result) {
            throw new IllegalArgumentException(String.format("failed to creat database [%s.%s]", catalogName, databaseName));
        }
    }

    public void dropSchema(String catalogName, String databaseName, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = String.format("DROP SCHEMA IF EXISTS %s.%s", catalogName, databaseName);
        Boolean result = StatementResultUtil.definitionStatement(queryRunner, statement);
        if (result == null || !result) {
            throw new IllegalArgumentException(String.format("failed to delete database [%s.%s]", catalogName, databaseName));
        }
    }
}
