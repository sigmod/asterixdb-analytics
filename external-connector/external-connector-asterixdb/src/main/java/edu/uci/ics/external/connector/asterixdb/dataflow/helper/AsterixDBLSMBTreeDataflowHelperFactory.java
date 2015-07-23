package edu.uci.ics.external.connector.asterixdb.dataflow.helper;

import java.util.Map;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;

public class AsterixDBLSMBTreeDataflowHelperFactory extends LSMBTreeDataflowHelperFactory {

    private static final long serialVersionUID = 1L;
    private final boolean needKeyDupCheck;
    private final int[] btreeFields;

    public AsterixDBLSMBTreeDataflowHelperFactory(IVirtualBufferCacheProvider virtualBufferCacheProvider,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationSchedulerProvider ioSchedulerProvider,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, double bloomFilterFalsePositiveRate,
            boolean needKeyDupCheck, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields, boolean durable) {
        super(virtualBufferCacheProvider, mergePolicyFactory, mergePolicyProperties, opTrackerFactory,
                ioSchedulerProvider, ioOpCallbackFactory, bloomFilterFalsePositiveRate, needKeyDupCheck,
                filterTypeTraits, filterCmpFactories, btreeFields, filterFields, durable);
        this.needKeyDupCheck = needKeyDupCheck;
        this.btreeFields = btreeFields;
    }

    @Override
    public IndexDataflowHelper createIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition) {
        return new AsterixDBLSMBTreeDataflowHelper(opDesc, ctx, partition,
                virtualBufferCacheProvider.getVirtualBufferCaches(ctx), bloomFilterFalsePositiveRate,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ctx), opTrackerFactory,
                ioSchedulerProvider.getIOScheduler(ctx), ioOpCallbackFactory, needKeyDupCheck, filterTypeTraits,
                filterCmpFactories, btreeFields, filterFields, durable);
    }

}
