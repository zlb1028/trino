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
package io.trino.plugin.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.ConnectorConfig;
import io.trino.spi.queryeditorui.ConnectorUtil;
import io.trino.spi.queryeditorui.ConnectorWithProperties;

import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@ConnectorConfig(propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true)
public class ElasticsearchPlugin
        implements Plugin
{
    private final ConnectorFactory connectorFactory;

    public ElasticsearchPlugin()
    {
        connectorFactory = new ElasticsearchConnectorFactory();
    }

    @VisibleForTesting
    ElasticsearchPlugin(ElasticsearchConnectorFactory factory)
    {
        connectorFactory = requireNonNull(factory, "factory is null");
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(connectorFactory);
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = ElasticsearchPlugin.class.getAnnotation(ConnectorConfig.class);
        return ConnectorUtil.assembleConnectorProperties(connectorConfig,
                Arrays.asList(ElasticsearchConfig.class.getDeclaredMethods()));
    }
}
