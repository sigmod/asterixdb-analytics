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

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.external.connector.api.IWriteConnector;
import edu.uci.ics.external.connector.asterixdb.api.FilePartition;
import edu.uci.ics.external.connector.asterixdb.api.IWriteConverterFactory;
import edu.uci.ics.external.connector.asterixdb.dataflow.WriteTransformOperatorDescriptor;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;

public class WriteConnector implements IWriteConnector {

    private final StorageParameter storageParameter;
    private final IWriteConverterFactory writeConverterFactory;

    private ARecordType recordType = null;
    private String[] locations = null;
    private List<FilePartition> filePartitions = null;

    public WriteConnector(StorageParameter storageParameter, IWriteConverterFactory writeConverterFactory) {
        this.storageParameter = storageParameter;
        this.writeConverterFactory = writeConverterFactory;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IOperatorDescriptor getWriteTransformOperatorDescriptor(JobSpecification jobSpec, String[] locations) {
        ISerializerDeserializer[] asterixFields = new ISerializerDeserializer[2];
        RecordDescriptor recordDescriptorAsterix = new RecordDescriptor(asterixFields, storageParameter.getTypeTraits());
        IOperatorDescriptor transformOperator = new WriteTransformOperatorDescriptor(jobSpec, recordDescriptorAsterix,
                recordType, writeConverterFactory);
        return transformOperator;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IOperatorDescriptor getWriteOperatorDescriptor(JobSpecification jobSpec, String[] locationConstraints) {
        // Retrieves the record type and the file partitions of the dataset from AsterixDB REST service.
        try {
            retrieveRecordTypeAndPartitions();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        // Creates AsterixDB file splits provider.
        IFileSplitProvider asterixFileSplitProvider = ConnectorUtils.createFileSplitProvider(storageParameter,
                filePartitions);

        ISerializerDeserializer[] asterixFields = new ISerializerDeserializer[2];
        RecordDescriptor recordDescriptorAsterix = new RecordDescriptor(asterixFields, storageParameter.getTypeTraits());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        int[] treeFields = new int[1];
        treeFields[0] = 0;

        IIndexDataflowHelperFactory asterixDataflowHelperFactory = new LSMBTreeDataflowHelperFactory(
                storageParameter.getVirtualBufferCacheProvider(), new ConstantMergePolicyFactory(),
                storageParameter.getMergePolicyProperties(), NoOpOperationTrackerProvider.INSTANCE,
                SynchronousSchedulerProvider.INSTANCE, NoOpIOOperationCallback.INSTANCE, 0.01, true,
                storageParameter.getTypeTraits(), null, null, null, false);

        // BTree Search operator.
        BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec,
                recordDescriptorAsterix, storageParameter.getStorageManagerInterface(),
                storageParameter.getIndexLifecycleManagerProvider(), asterixFileSplitProvider,
                storageParameter.getTypeTraits(), comparatorFactories, treeFields, null, null, true, true,
                asterixDataflowHelperFactory, false, false, null, NoOpOperationCallbackFactory.INSTANCE, null, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, btreeSearchOp, locations);
        return btreeSearchOp;
    }

    // Retrieves the type and partition information of the target AsterixDB dataset.
    private void retrieveRecordTypeAndPartitions() throws Exception {
        if (recordType != null && locations != null && filePartitions != null) {
            return;
        }
        // Extracts record type and file partitions.
        Pair<ARecordType, String[]> typeAndConstraints = ConnectorUtils
                .retrieveRecordTypeAndPartitions(storageParameter);
        this.recordType = typeAndConstraints.getLeft();
        this.locations = typeAndConstraints.getRight();
    }

}
