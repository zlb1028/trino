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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.queryeditorui.QueryEditorUIModule;
import io.trino.queryeditorui.execution.QueryRunner;
import io.trino.queryeditorui.protocol.ColumnInfo;
import io.trino.queryeditorui.protocol.TableInfo;
import io.trino.queryeditorui.util.StatementResultUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author lipusheng
 * @date 2021/4/12
 */
public class TableService
{
    private static final Logger LOG = Logger.get(TableService.class);

    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;

    @Inject
    public TableService(final QueryRunner.QueryRunnerFactory queryRunnerFactory)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory session was null!");
    }

    public void createTable(TableInfo table, String user)
    {
        String catalogName = table.getCatalog();
        String schemaName = table.getSchema();
        String tableName = table.getTable();
        List<ColumnInfo> columns = table.getColumns();
        Map<String, String> parameters = table.getParameters();
        String tableColumnsStr = columns.stream()
                .map(column -> (String.format("%s %s", column.getName(), column.getType())))
                .collect(Collectors.joining(", "));
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = format("CREATE TABLE IF NOT EXISTS %s.%s.%s(%s)", catalogName, schemaName, tableName, tableColumnsStr);
        if (parameters != null && !parameters.isEmpty()) {
            String propertiesStr = parameters.entrySet().stream()
                    .map(param -> String.format("%s=%s", param.getKey(), param.getValue()))
                    .collect(Collectors.toList())
                    .stream()
                    .collect(Collectors.joining(","));
            statement = String.format("%s WITH (%s)", statement, propertiesStr);
        }
        Boolean result = StatementResultUtil.definitionStatement(queryRunner, statement);
        if (result == null || !result) {
            throw new IllegalArgumentException(String.format("failed to create table: [%s.%s.%s], please check you arguments", catalogName, schemaName, tableName));
        }
    }

    public void dropTable(TableInfo table, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String catalogName = table.getCatalog();
        String schemaName = table.getSchema();
        String tableName = table.getTable();
        String statement = String.format("DROP TABLE IF EXISTS %s.%s.%s", catalogName, schemaName, tableName);
        Boolean result = StatementResultUtil.definitionStatement(queryRunner, statement);
        if (result == null || !result) {
            throw new IllegalArgumentException(String.format("failed to drop table: [%s.%s.%s], please check you arguments", catalogName, schemaName, tableName));
        }
    }

    public void updateTableName(TableInfo table, String newTable, String user)
    {
        String catalogName = table.getCatalog();
        String schemaName = table.getSchema();
        String tableName = table.getTable();
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, user);
        String statement = String.format("ALTER TABLE %s.%s.%s RENAME TO %s", catalogName, schemaName, tableName, newTable);
        Boolean result = StatementResultUtil.definitionStatement(queryRunner, statement);
        if (result == null || !result) {
            throw new IllegalArgumentException(String.format("failed to update table: [%s.%s.%s], please check you arguments", catalogName, schemaName, tableName));
        }
    }
}
