/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.external.connector.asterixdb;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

public class StorageParameter {

    private final IVirtualBufferCacheProvider bufferCacheProvider;
    private final IStorageManagerInterface storageManagerInterface;
    private final IIndexLifecycleManagerProvider indexLifecycleManagerProvider;
    private final Map<String, String> mergePolicyProperties;

    private final String serviceURL;
    private final String dataverseName;
    private final String datasetName;

    private final Map<String, List<String>> ipToNcNames;

    public StorageParameter(IVirtualBufferCacheProvider bufferCacheProvider,
            IStorageManagerInterface storageManagerInterface,
            IIndexLifecycleManagerProvider indexLifecycleManagerProvider, Map<String, String> mergePolicyProperties,
            String serviceURL, String dataverseName, String datasetName, Map<String, List<String>> ipToNcNames) {
        this.bufferCacheProvider = bufferCacheProvider;
        this.storageManagerInterface = storageManagerInterface;
        this.indexLifecycleManagerProvider = indexLifecycleManagerProvider;
        this.mergePolicyProperties = mergePolicyProperties;
        this.serviceURL = serviceURL;
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.ipToNcNames = ipToNcNames;
    }

    public IVirtualBufferCacheProvider getVirtualBufferCacheProvider() {
        return bufferCacheProvider;
    }

    public IStorageManagerInterface getStorageManagerInterface() {
        return storageManagerInterface;
    }

    public IIndexLifecycleManagerProvider getIndexLifecycleManagerProvider() {
        return indexLifecycleManagerProvider;
    }

    public Map<String, String> getMergePolicyProperties() {
        return mergePolicyProperties;
    }

    public String getServiceURL() {
        return serviceURL;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public Map<String, List<String>> getIpToNcNames() {
        return ipToNcNames;
    }

}
