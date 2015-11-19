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

package edu.uci.ics.pregelix.dataflow;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.SendSideMaterializedPipeliningConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.SendSidePipeliningReceiveSideMaterializedBlockingConnectorPolicy;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;

public class ConnectorPolicyAssignmentPolicy implements IConnectorPolicyAssignmentPolicy {
    private static final long serialVersionUID = 1L;
    private final IConnectorPolicy senderSideMatPipPolicy = new SendSideMaterializedPipeliningConnectorPolicy();
    private final IConnectorPolicy senderSidePipeliningReceiverSideMatBlkPolicy = new SendSidePipeliningReceiveSideMaterializedBlockingConnectorPolicy();
    private final IConnectorPolicy pipeliningPolicy = new PipeliningConnectorPolicy();
    private final JobSpecification spec;

    public ConnectorPolicyAssignmentPolicy(JobSpecification spec) {
        this.spec = spec;
    }

    @Override
    public IConnectorPolicy getConnectorPolicyAssignment(IConnectorDescriptor c, int nProducers, int nConsumers,
            int[] fanouts) {
        if (c.getClass().getName().contains("MToNPartitioningMergingConnectorDescriptor")) {
            return senderSideMatPipPolicy;
        } else {
            Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> endPoints = spec
                    .getConnectorOperatorMap().get(c.getConnectorId());
            IOperatorDescriptor consumer = endPoints.getRight().getLeft();
            if (consumer instanceof TreeIndexInsertUpdateDeleteOperatorDescriptor) {
                return senderSidePipeliningReceiverSideMatBlkPolicy;
            } else {
                return pipeliningPolicy;
            }
        }
    }
}
