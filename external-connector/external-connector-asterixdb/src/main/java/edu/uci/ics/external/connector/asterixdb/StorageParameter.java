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

import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class StorageParameter {

    private final IVirtualBufferCacheProvider bufferCacheProvider;
    private final IStorageManagerInterface storageManagerInterface;
    private final IIndexLifecycleManagerProvider indexLifecycleManagerProvider;
    private final Map<String, String> mergePolicyProperties;
    private final ITypeTraits[] typeTraits;

    private final String serviceURL;
    private final String dataverseName;
    private final String datasetName;

    private final Map<String, String> ipToNcNames;

    private final int[] sortFields;
    private final int[] fieldPermutation;

    public StorageParameter(IVirtualBufferCacheProvider bufferCacheProvider,
            IStorageManagerInterface storageManagerInterface,
            IIndexLifecycleManagerProvider indexLifecycleManagerProvider, Map<String, String> mergePolicyProperties,
            ITypeTraits[] typeTraits, String serviceURL, String dataverseName, String datasetName,
            Map<String, String> ipToNcNames, int[] sortFields, int[] fieldPermutation) {
        this.bufferCacheProvider = bufferCacheProvider;
        this.storageManagerInterface = storageManagerInterface;
        this.indexLifecycleManagerProvider = indexLifecycleManagerProvider;
        this.mergePolicyProperties = mergePolicyProperties;
        this.typeTraits = typeTraits;
        this.serviceURL = serviceURL;
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.ipToNcNames = ipToNcNames;
        this.sortFields = sortFields;
        this.fieldPermutation = fieldPermutation;
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

    public ITypeTraits[] getTypeTraits() {
        return typeTraits;
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

    public Map<String, String> getIpToNcNames() {
        return ipToNcNames;
    }

    public int[] getSortFields() {
        return sortFields;
    }

    public int[] getFieldPermutation() {
        return fieldPermutation;
    }

}
