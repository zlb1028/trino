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
package io.trino.queryeditorui.output.persistors;

import io.trino.queryeditorui.output.PersistentJobOutput;
import io.trino.queryeditorui.protocol.Job;

public class PersistorFactory
{
    private final CSVPersistorFactory csvPersistorFactory;

    public PersistorFactory(CSVPersistorFactory csvPersistorFactory)
    {
        this.csvPersistorFactory = csvPersistorFactory;
    }

    public Persistor getPersistor(Job job, PersistentJobOutput jobOutput)
    {
        switch (jobOutput.getType()) {
            case "csv":
                return csvPersistorFactory.getPersistor(job, jobOutput);
            default:
                throw new IllegalArgumentException();
        }
    }
}
