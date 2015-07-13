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

import edu.uci.ics.external.connector.api.IReadConnector;
import edu.uci.ics.external.connector.asterixdb.api.IReadConverterFactory;
import edu.uci.ics.external.connector.asterixdb.dataflow.ReadTransformOperatorDescriptor;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;

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
    public IOperatorDescriptor getReadTransformOperatorDescriptor(JobSpecification jobSpec, String[] locations) {
        IOperatorDescriptor transformOperator = new ReadTransformOperatorDescriptor(jobSpec,
                datasetInfo.getRecordDescriptor(), datasetInfo.getRecordType(), readConverterFactory);
        return transformOperator;
    }

    @Override
    public IOperatorDescriptor getReadOperatorDescriptor(JobSpecification jobSpec, String[] locationConstraints) {
        IIndexDataflowHelperFactory asterixDataflowHelperFactory = new LSMBTreeDataflowHelperFactory(
                storageParameter.getVirtualBufferCacheProvider(), new ConstantMergePolicyFactory(),
                storageParameter.getMergePolicyProperties(), NoOpOperationTrackerProvider.INSTANCE,
                SynchronousSchedulerProvider.INSTANCE, NoOpIOOperationCallback.INSTANCE, 0.01, true,
                storageParameter.getTypeTraits(), null, null, null, false);

        // BTree Search operator.
        BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec,
                datasetInfo.getRecordDescriptor(), storageParameter.getStorageManagerInterface(),
                storageParameter.getIndexLifecycleManagerProvider(), datasetInfo.getFileSplitProvider(),
                storageParameter.getTypeTraits(), datasetInfo.getPrimaryKeyComparatorFactories(),
                datasetInfo.getSortFields(), null, null, true, true, asterixDataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, null, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, btreeSearchOp,
                datasetInfo.getLocationConstraints());
        return btreeSearchOp;
    }

}
