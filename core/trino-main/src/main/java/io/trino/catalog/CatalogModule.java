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

package io.trino.catalog;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.FileSystemClientManager;
import io.trino.security.CipherTextDecryptUtil;
import io.trino.security.KeystoreSecurityKeyManager;
import io.trino.security.NoneCipherTextDecrypt;
import io.trino.security.PasswordSecurityConfig;
import io.trino.spi.security.CipherTextDecrypt;
import io.trino.spi.security.SecurityKeyManager;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class CatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jaxrsBinder(binder).bind(CatalogResource.class);
        jaxrsBinder(binder).bind(MultiPartFeature.class);
        binder.bind(DynamicCatalogStore.class).in(Scopes.SINGLETON);
        binder.bind(SecurityKeyManager.class)
                .to(KeystoreSecurityKeyManager.class);
        binder.bind(CatalogStoreUtil.class).in(Scopes.SINGLETON);
        binder.bind(FileSystemClientManager.class).in(Scopes.SINGLETON);
        binder.bind(CipherTextDecryptUtil.class).in(Scopes.SINGLETON);
        binder.bind(PasswordSecurityConfig.class).in(Scopes.SINGLETON);
        binder.bind(CipherTextDecrypt.class).to(NoneCipherTextDecrypt.class).in(Scopes.SINGLETON);
        binder.bind(DynamicCatalogService.class).in(Scopes.SINGLETON);
    }
}
