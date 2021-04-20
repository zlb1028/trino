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
package io.trino.queryeditorui.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author 4paradigm
 * @date 2021/4/12
 */
@Immutable
public class TableInfo
{
    private String catalog;
    private String schema;
    private String table;
    private List<ColumnInfo> columns;
    private Map<String, String> parameters;

    @JsonCreator
    public TableInfo(String catalog, String schema, String table, List<ColumnInfo> columns,
                     Map<String, String> parameters)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.parameters = parameters;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    @JsonProperty
    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    public void setColumns(List<ColumnInfo> columns)
    {
        this.columns = columns;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters)
    {
        this.parameters = parameters;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableInfo that = (TableInfo) o;
        return catalog.equals(that.catalog) &&
                schema.equals(that.schema) &&
                table.equals(that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema, table);
    }
}
