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

import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;

import edu.uci.ics.external.connector.api.IReadConnector;
import edu.uci.ics.external.connector.api.ParallelOperator;
import edu.uci.ics.external.connector.asterixdb.api.IReadConverterFactory;
import edu.uci.ics.external.connector.asterixdb.dataflow.AsterixDBBTreeSearchOperatorDescriptor;
import edu.uci.ics.external.connector.asterixdb.dataflow.ReadTransformOperatorDescriptor;
import edu.uci.ics.external.connector.asterixdb.dataflow.helper.AsterixDBLSMBTreeDataflowHelperFactory;

public class ReadConnector implements IReadConnector {

    private final StorageParameter storageParameter;
    private final IReadConverterFactory readConverterFactory;
    private DatasetInfo datasetInfo = null;

    public ReadConnector(StorageParameter storageParameter, IReadConverterFactory readConverterFactory) {
        this.storageParameter = storageParameter;
        this.readConverterFactory = readConverterFactory;
        try {
            // Retrieve dataset info from the AsterixDB REST service.
            datasetInfo = ConnectorUtils.retrieveDatasetInfo(storageParameter);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public ParallelOperator getReadTransformOperatorDescriptor(JobSpecification jobSpec, String[] locations) {
        IOperatorDescriptor transformOperator = new ReadTransformOperatorDescriptor(jobSpec,
                datasetInfo.getRecordDescriptor(), datasetInfo.getRecordType(), readConverterFactory);
        return new ParallelOperator(transformOperator, datasetInfo.getLocationConstraints());
    }

    @Override
    public ParallelOperator getReadOperatorDescriptor(JobSpecification jobSpec, String[] locationConstraints) {
        IIndexDataflowHelperFactory asterixDataflowHelperFactory = new AsterixDBLSMBTreeDataflowHelperFactory(
                storageParameter.getVirtualBufferCacheProvider(), new ConstantMergePolicyFactory(),
                storageParameter.getMergePolicyProperties(), NoOpOperationTrackerProvider.INSTANCE,
                SynchronousSchedulerProvider.INSTANCE, NoOpIOOperationCallback.INSTANCE, 0.01, true,
                datasetInfo.getTypeTraits(), null, null, null, false);

        // BTree Search operator.
        AsterixDBBTreeSearchOperatorDescriptor btreeSearchOp = new AsterixDBBTreeSearchOperatorDescriptor(jobSpec,
                datasetInfo.getRecordDescriptor(), storageParameter.getStorageManagerInterface(),
                storageParameter.getIndexLifecycleManagerProvider(), datasetInfo.getFileSplitProvider(),
                datasetInfo.getTypeTraits(), datasetInfo.getPrimaryKeyComparatorFactories(),
                datasetInfo.getSortFields(), null, null, true, true, asterixDataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, null, null);
        return new ParallelOperator(btreeSearchOp, datasetInfo.getLocationConstraints());
    }

}
