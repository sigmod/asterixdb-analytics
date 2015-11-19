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
package edu.uci.ics.pregelix.dataflow.std;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.TransientLocalResourceFactoryProvider;

public class IndexNestedLoopJoinOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private boolean isForward;
    private int[] lowKeyFields; // fields in input tuple to be used as low keys
    private int[] highKeyFields; // fields in input tuple to be used as high
    // keys
    private boolean lowKeyInclusive;
    private boolean highKeyInclusive;

    // right outer join
    private boolean isRightOuter = false;
    private INullWriterFactory[] nullWriterFactories;

    // set union
    private boolean isSetUnion = false;

    public IndexNestedLoopJoinOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lcManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, boolean isForward, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, IIndexDataflowHelperFactory opHelperFactory) {
        super(spec, 1, 1, recDesc, storageManager, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, opHelperFactory, null, false, false, null,
                TransientLocalResourceFactoryProvider.INSTANCE, NoOpOperationCallbackFactory.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE);
        this.isForward = isForward;
        this.lowKeyFields = lowKeyFields;
        this.highKeyFields = highKeyFields;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
    }

    public IndexNestedLoopJoinOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lcManagerProvider,
            IFileSplitProvider fileSplitProvider, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, boolean isForward, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, IIndexDataflowHelperFactory opHelperFactory,
            boolean isRightOuter, INullWriterFactory[] nullWriterFactories) {
        super(spec, 1, 1, recDesc, storageManager, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, opHelperFactory, null, false, false, null,
                TransientLocalResourceFactoryProvider.INSTANCE, NoOpOperationCallbackFactory.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE);
        this.isForward = isForward;
        this.lowKeyFields = lowKeyFields;
        this.highKeyFields = highKeyFields;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;

        this.isRightOuter = isRightOuter;
        this.nullWriterFactories = nullWriterFactories;
    }

    public IndexNestedLoopJoinOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lcManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, boolean isForward, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, IIndexDataflowHelperFactory opHelperFactory,
            boolean isSetUnion) {
        super(spec, 1, 1, recDesc, storageManager, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, opHelperFactory, null, false, false, null,
                TransientLocalResourceFactoryProvider.INSTANCE, NoOpOperationCallbackFactory.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE);
        this.isForward = isForward;
        this.lowKeyFields = lowKeyFields;
        this.highKeyFields = highKeyFields;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;

        this.isSetUnion = isSetUnion;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        if (isRightOuter) {
            INullWriter[] nullWriters = new INullWriter[nullWriterFactories.length];
            for (int i = 0; i < nullWriters.length; i++)
                nullWriters[i] = nullWriterFactories[i].createNullWriter();
            return new IndexNestedLoopRightOuterJoinOperatorNodePushable(this, ctx, partition, recordDescProvider,
                    isForward, lowKeyFields, highKeyFields, nullWriters);
        } else if (isSetUnion) {
            return new IndexNestedLoopSetUnionOperatorNodePushable(this, ctx, partition, recordDescProvider, isForward,
                    lowKeyFields, highKeyFields);
        } else {
            return new IndexNestedLoopJoinOperatorNodePushable(this, ctx, partition, recordDescProvider, isForward,
                    lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive);
        }
    }
}