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

package edu.uci.ics.pregelix.runtime.agg;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;

public class AggregationEvaluatorFactory implements IAggregateEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;
    private final boolean isFinalStage;
    private final boolean partialAggAsInput;

    public AggregationEvaluatorFactory(IConfigurationFactory confFactory, boolean isFinalStage,
            boolean partialAggAsInput) {
        this.confFactory = confFactory;
        this.isFinalStage = isFinalStage;
        this.partialAggAsInput = partialAggAsInput;
    }

    @Override
    public IAggregateEvaluator createAggregateEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        try {
            return new AggregationEvaluator(ctx, confFactory, isFinalStage, partialAggAsInput);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }
}
