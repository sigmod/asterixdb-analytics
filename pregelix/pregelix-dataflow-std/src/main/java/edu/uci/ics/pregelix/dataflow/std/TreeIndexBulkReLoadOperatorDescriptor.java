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
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.TransientLocalResourceFactoryProvider;

public class TreeIndexBulkReLoadOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int[] fieldPermutation;
    private final IStorageManagerInterface storageManager;
    private final IIndexLifecycleManagerProvider lcManagerProvider;
    private final IFileSplitProvider fileSplitProvider;
    private final float fillFactor;

    public TreeIndexBulkReLoadOperatorDescriptor(JobSpecification spec, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lcManagerProvider, IFileSplitProvider fileSplitProvider,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] comparatorFactories, int[] fieldPermutation,
            int[] bloomFilterFields, float fillFactor, IIndexDataflowHelperFactory opHelperFactory) {
        super(spec, 1, 0, null, storageManager, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories,
                bloomFilterFields, opHelperFactory, null, false, false, null,
                new TransientLocalResourceFactoryProvider(), NoOpOperationCallbackFactory.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE);
        this.fieldPermutation = fieldPermutation;

        this.storageManager = storageManager;
        this.lcManagerProvider = lcManagerProvider;
        this.fileSplitProvider = fileSplitProvider;
        this.fillFactor = fillFactor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new TreeIndexBulkReLoadOperatorNodePushable(this, ctx, partition, fieldPermutation, fillFactor,
                recordDescProvider, storageManager, lcManagerProvider, fileSplitProvider);
    }
}