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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.thrift.StaticMetastoreConfig;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.ConnectorConfig;
import io.trino.spi.queryeditorui.ConnectorUtil;
import io.trino.spi.queryeditorui.ConnectorWithProperties;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

@ConnectorConfig(propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true)
public class HiveHadoop2Plugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new HiveConnectorFactory("hive-hadoop2"));
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = HiveHadoop2Plugin.class.getAnnotation(ConnectorConfig.class);
        ArrayList<Method> methods = new ArrayList<>();
        methods.addAll(Arrays.asList(StaticMetastoreConfig.class.getDeclaredMethods()));
        methods.addAll(Arrays.asList(HiveConfig.class.getDeclaredMethods()));
        return ConnectorUtil.assembleConnectorProperties(connectorConfig, methods);
    }
}
