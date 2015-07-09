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
import edu.uci.ics.external.connector.asterixdb.api.IWriteConverterFactory;
import edu.uci.ics.external.connector.asterixdb.dataflow.WriteTransformOperatorDescriptor;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
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
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        try {
            retrieveRecordTypeAndPartitions();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return datasetInfo.getPrimaryKeyComparatorFactories();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IOperatorDescriptor getWriteTransformOperatorDescriptor(JobSpecification jobSpec, String[] locations) {
        try {
            retrieveRecordTypeAndPartitions();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        ISerializerDeserializer[] asterixFields = new ISerializerDeserializer[2];
        RecordDescriptor recordDescriptorAsterix = new RecordDescriptor(asterixFields, storageParameter.getTypeTraits());
        IOperatorDescriptor transformOperator = new WriteTransformOperatorDescriptor(jobSpec, recordDescriptorAsterix,
                datasetInfo.getRecordType(), writeConverterFactory);
        return transformOperator;
    }

    @Override
    public IOperatorDescriptor getWriteOperatorDescriptor(JobSpecification jobSpec, String[] locationConstraints) {
        // Retrieves the record type and the file partitions of the dataset from AsterixDB REST service.
        try {
            retrieveRecordTypeAndPartitions();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        IIndexDataflowHelperFactory asterixDataflowHelperFactory = new LSMBTreeDataflowHelperFactory(
                storageParameter.getVirtualBufferCacheProvider(), new ConstantMergePolicyFactory(),
                storageParameter.getMergePolicyProperties(), NoOpOperationTrackerProvider.INSTANCE,
                SynchronousSchedulerProvider.INSTANCE, NoOpIOOperationCallback.INSTANCE, 0.01, true,
                storageParameter.getTypeTraits(), null, null, null, false);

        // BTree bulkload operator.
        TreeIndexBulkLoadOperatorDescriptor writer = new TreeIndexBulkLoadOperatorDescriptor(jobSpec,
                datasetInfo.getRecordDescriptor(), storageParameter.getStorageManagerInterface(),
                storageParameter.getIndexLifecycleManagerProvider(), datasetInfo.getFileSplitProvider(),
                storageParameter.getTypeTraits(), getComparatorFactories(), storageParameter.getSortFields(),
                storageParameter.getFieldPermutation(), DEFAULT_BTREE_FILL_FACTOR, true, BF_HINT, false,
                asterixDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, writer, datasetInfo.getLocationConstraints());
        return null;
    }

    // Retrieves the type and partition information of the target AsterixDB dataset.
    private void retrieveRecordTypeAndPartitions() throws Exception {
        if (datasetInfo == null) {
            return;
        }
        // Extracts record type and file partitions.
        datasetInfo = ConnectorUtils.retrieveDatasetInfo(storageParameter);
    }

}
