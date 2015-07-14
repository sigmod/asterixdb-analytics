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

import edu.uci.ics.external.connector.api.IWriteConnector;
import edu.uci.ics.external.connector.api.ParallelOperator;
import edu.uci.ics.external.connector.asterixdb.api.IWriteConverterFactory;
import edu.uci.ics.external.connector.asterixdb.dataflow.WriteTransformOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;

public class WriteConnector implements IWriteConnector {

    private static final int BF_HINT = 100000;
    private static final float DEFAULT_BTREE_FILL_FACTOR = 1.00f;

    private final StorageParameter storageParameter;
    private final IWriteConverterFactory writeConverterFactory;

    private DatasetInfo datasetInfo = null;

    public WriteConnector(StorageParameter storageParameter, IWriteConverterFactory writeConverterFactory) {
        this.storageParameter = storageParameter;
        this.writeConverterFactory = writeConverterFactory;
        try {
            // Retrieve dataset info from the AsterixDB REST service.
            datasetInfo = ConnectorUtils.retrieveDatasetInfo(storageParameter);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return datasetInfo.getPrimaryKeyComparatorFactories();
    }

    @Override
    public ParallelOperator getWriteTransformOperatorDescriptor(JobSpecification jobSpec, String[] locations) {
        IOperatorDescriptor transformOperator = new WriteTransformOperatorDescriptor(jobSpec,
                datasetInfo.getRecordDescriptor(), datasetInfo.getRecordType(), writeConverterFactory);
        return new ParallelOperator(transformOperator, datasetInfo.getLocationConstraints());
    }

    @Override
    public ParallelOperator getWriteOperatorDescriptor(JobSpecification jobSpec, String[] locationConstraints) {
        IIndexDataflowHelperFactory asterixDataflowHelperFactory = new LSMBTreeDataflowHelperFactory(
                storageParameter.getVirtualBufferCacheProvider(), new ConstantMergePolicyFactory(),
                storageParameter.getMergePolicyProperties(), NoOpOperationTrackerProvider.INSTANCE,
                SynchronousSchedulerProvider.INSTANCE, NoOpIOOperationCallback.INSTANCE, 0.01, true,
                datasetInfo.getTypeTraits(), null, null, null, false);

        // BTree bulkload operator.
        TreeIndexBulkLoadOperatorDescriptor writer = new TreeIndexBulkLoadOperatorDescriptor(jobSpec,
                datasetInfo.getRecordDescriptor(), storageParameter.getStorageManagerInterface(),
                storageParameter.getIndexLifecycleManagerProvider(), datasetInfo.getFileSplitProvider(),
                datasetInfo.getTypeTraits(), getComparatorFactories(), datasetInfo.getSortFields(),
                datasetInfo.getFieldPermutation(), DEFAULT_BTREE_FILL_FACTOR, true, BF_HINT, false,
                asterixDataflowHelperFactory);
        return new ParallelOperator(writer, datasetInfo.getLocationConstraints());
    }
}
