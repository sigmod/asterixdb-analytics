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

package edu.uci.ics.external.connector.asterixdb.dataflow.helper;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public class AsterixDBLSMBTreeDataflowHelper extends LSMBTreeDataflowHelper {

    public AsterixDBLSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            boolean needKeyDupCheck, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields, boolean durable) {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, needKeyDupCheck, filterTypeTraits, filterCmpFactories, btreeFields,
                filterFields, durable);
    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (lcManager) {
            long resourceID = getResourceID();
            // Closes the resource first.
            lcManager.close(resourceID);

            // Cleanup things associated with the resource.
            index = lcManager.getIndex(resourceID);
            if (index != null) {
                // Unregister will deactivate the index.
                // TODO(yingyib): there is still a leak in the FileMapManager.
                lcManager.unregister(resourceID);
            }

            if (resourceID != -1) {
                localResourceRepository.deleteResourceByName(file.getFile().getPath());
            }
        }
    }
}
