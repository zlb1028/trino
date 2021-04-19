/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.trino.queryeditorui;

import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.units.Duration;
import io.trino.client.SocketChannelSocketFactory;
import io.trino.queryeditorui.execution.ClientSessionFactory;
import io.trino.queryeditorui.execution.ExecutionClient;
import io.trino.queryeditorui.execution.QueryInfoClient;
import io.trino.queryeditorui.execution.QueryRunner.QueryRunnerFactory;
import io.trino.queryeditorui.metadata.ColumnService;
import io.trino.queryeditorui.metadata.PreviewTableService;
import io.trino.queryeditorui.metadata.SchemaService;
import io.trino.queryeditorui.output.PersistentJobOutputFactory;
import io.trino.queryeditorui.output.builders.OutputBuilderFactory;
import io.trino.queryeditorui.output.persistors.CSVPersistorFactory;
import io.trino.queryeditorui.output.persistors.PersistorFactory;
import io.trino.queryeditorui.protocol.ExecutionStatus.ExecutionError;
import io.trino.queryeditorui.protocol.ExecutionStatus.ExecutionSuccess;
import io.trino.queryeditorui.resources.ConnectorResource;
import io.trino.queryeditorui.resources.FilesResource;
import io.trino.queryeditorui.resources.LoginResource;
import io.trino.queryeditorui.resources.MetadataResource;
import io.trino.queryeditorui.resources.QueryResource;
import io.trino.queryeditorui.resources.ResultsPreviewResource;
import io.trino.queryeditorui.resources.UIExecuteResource;
import io.trino.queryeditorui.resources.UserResource;
import io.trino.queryeditorui.security.UiAuthenticator;
import io.trino.queryeditorui.store.files.ExpiringFileStore;
import io.trino.queryeditorui.store.history.JobHistoryStore;
import io.trino.queryeditorui.store.history.LocalJobHistoryStore;
import io.trino.queryeditorui.store.jobs.jobs.ActiveJobsStore;
import io.trino.queryeditorui.store.jobs.jobs.InMemoryActiveJobsStore;
import io.trino.queryeditorui.store.queries.InMemoryQueryStore;
import io.trino.queryeditorui.store.queries.QueryStore;
import io.trino.server.InternalAuthenticationManager;
import io.trino.server.InternalCommunicationConfig;
import io.trino.server.security.PasswordAuthenticatorConfig;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.security.WebUIAuthenticator;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

import javax.inject.Named;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.client.OkHttpUtil.setupCookieJar;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.client.OkHttpUtil.setupTimeouts;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryEditorUIModule
        extends AbstractConfigurationAwareModule
{
    public static final String UI_QUERY_SOURCE = "ui-server";
    private static final ConfigDefaults<HttpClientConfig> HTTP_CLIENT_CONFIG_DEFAULTS = d -> new HttpClientConfig()
            .setConnectTimeout(new Duration(10, TimeUnit.SECONDS));

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(QueryEditorConfig.class);

        //resources
        jsonCodecBinder(binder).bindJsonCodec(ExecutionSuccess.class);
        jsonCodecBinder(binder).bindJsonCodec(ExecutionError.class);
        jaxrsBinder(binder).bind(UIExecuteResource.class);
        jaxrsBinder(binder).bind(FilesResource.class);
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(ResultsPreviewResource.class);
        jaxrsBinder(binder).bind(MetadataResource.class);
        jaxrsBinder(binder).bind(ConnectorResource.class);
        jaxrsBinder(binder).bind(LoginResource.class);
        jaxrsBinder(binder).bind(UserResource.class);

        binder.bind(PasswordAuthenticatorManager.class).in(Scopes.SINGLETON);
        binder.bind(PasswordAuthenticatorConfig.class).in(Scopes.SINGLETON);
        binder.bind(SchemaService.class).in(Scopes.SINGLETON);
        binder.bind(ColumnService.class).in(Scopes.SINGLETON);
        binder.bind(PreviewTableService.class).in(Scopes.SINGLETON);
        binder.bind(ExecutionClient.class).in(Scopes.SINGLETON);
        binder.bind(PersistentJobOutputFactory.class).in(Scopes.SINGLETON);
        binder.bind(JobHistoryStore.class).to(LocalJobHistoryStore.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("query-info", ForQueryInfoClient.class)
                .withConfigDefaults(HTTP_CLIENT_CONFIG_DEFAULTS);
        binder.bind(WebUIAuthenticator.class).to(UiAuthenticator.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public ExpiringFileStore provideExpiringFileStore(QueryEditorConfig config)
    {
        return new ExpiringFileStore(config.getMaxResultCount());
    }

    @Provides
    @Singleton
    public CSVPersistorFactory provideCSVPersistorFactory(ExpiringFileStore fileStore)
    {
        return new CSVPersistorFactory(fileStore);
    }

    @Provides
    @Singleton
    public PersistorFactory providePersistorFactory(CSVPersistorFactory csvPersistorFactory)
    {
        return new PersistorFactory(csvPersistorFactory);
    }

    @Provides
    @Singleton
    public ActiveJobsStore provideActiveJobsStore()
    {
        return new InMemoryActiveJobsStore();
    }

    @Provides
    @Singleton
    public OutputBuilderFactory provideOutputBuilderFactory(QueryEditorConfig config)
    {
        long maxFileSizeInBytes = Math.round(Math.floor(config.getMaxResultSize().toBytes()));
        return new OutputBuilderFactory(maxFileSizeInBytes, false);
    }

    @Singleton
    @Provides
    public OkHttpClient provideOkHttpClient(HttpServerConfig serverConfig, InternalCommunicationConfig internalCommunicationConfig,
            InternalAuthenticationManager internalAuthenticationManager)
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        builder.socketFactory(new SocketChannelSocketFactory());

        setupTimeouts(builder, 30, SECONDS);
        setupCookieJar(builder);
        if (serverConfig.isHttpsEnabled() && internalCommunicationConfig.isHttpsRequired()) {
            setupSsl(builder, Optional.of(serverConfig.getKeystorePath()), Optional.of(serverConfig.getKeystorePassword()),
                    Optional.empty(), Optional.of(serverConfig.getTrustStorePath()), Optional.of(serverConfig.getTrustStorePassword()), Optional.empty());

            //Setup the authorization for UI queries
            Interceptor authInterceptor = chain -> chain.proceed(
                                    chain.request().newBuilder()
                                            .header(InternalAuthenticationManager.TRINO_INTERNAL_BEARER, internalAuthenticationManager.generateJwt())
                                            .build());
            builder.addInterceptor(authInterceptor);
        }
        return builder.build();
    }

    @Named("coordinator-uri")
    @Provides
    public URI providePrestoCoordinatorURI(HttpServerConfig serverConfig, InternalCommunicationConfig internalCommunicationConfig,
            QueryEditorConfig queryEditorConfig)
    {
        if (queryEditorConfig.isRunningEmbeded()) {
            if (serverConfig.isHttpsEnabled() && internalCommunicationConfig.isHttpsRequired()) {
                return URI.create("https://localhost:" + serverConfig.getHttpsPort());
            }
            return URI.create("http://localhost:" + serverConfig.getHttpPort());
        }
        else {
            return URI.create(queryEditorConfig.getCoordinatorUri());
        }
    }

    @Singleton
    @Named("default-catalog")
    @Provides
    public String provideDefaultCatalog()
    {
        return "hive";
    }

    @Provides
    @Singleton
    public ClientSessionFactory provideClientSessionFactory(@Named("coordinator-uri") Provider<URI> uriProvider)
    {
        return new ClientSessionFactory(uriProvider,
                "lk",
                "ui",
                "system",
                "information_schema",
                Duration.succinctDuration(15, TimeUnit.MINUTES));
    }

    @Provides
    public QueryRunnerFactory provideQueryRunner(ClientSessionFactory sessionFactory,
            OkHttpClient httpClient)
    {
        return new QueryRunnerFactory(sessionFactory, httpClient);
    }

    @Provides
    public QueryInfoClient provideQueryInfoClient(OkHttpClient httpClient)
    {
        return new QueryInfoClient(httpClient);
    }

    @Provides
    public QueryStore provideQueryStore(QueryEditorConfig queryEditorConfig) throws IOException
    {
        return new InMemoryQueryStore(new File(queryEditorConfig.getFeaturedQueriesPath()), new File(queryEditorConfig.getUserQueriesPath()));
    }
}
