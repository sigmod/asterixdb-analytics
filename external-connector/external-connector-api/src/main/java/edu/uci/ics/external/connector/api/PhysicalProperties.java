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

package edu.uci.ics.external.connector.api;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class PhysicalProperties {

    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final INormalizedKeyComputerFactory normalizedKeyComputerFactory;
    private final RecordDescriptor recordDescriptor;

    public PhysicalProperties(IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] binaryHashFunctionFactories,
            INormalizedKeyComputerFactory normalizedKeyComputerFactory, RecordDescriptor recordDescriptor) {
        this.comparatorFactories = comparatorFactories;
        this.hashFunctionFactories = binaryHashFunctionFactories;
        this.normalizedKeyComputerFactory = normalizedKeyComputerFactory;
        this.recordDescriptor = recordDescriptor;
    }

    public IBinaryHashFunctionFactory[] getBinaryHashFunctionFactories() {
        return hashFunctionFactories;
    }

    public INormalizedKeyComputerFactory getNormalizedKeyComputerFactory() {
        return normalizedKeyComputerFactory;
    }

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return comparatorFactories;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

}
