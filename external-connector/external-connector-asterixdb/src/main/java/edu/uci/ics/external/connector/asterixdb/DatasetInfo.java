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

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlNormalizedKeyComputerFactoryProvider;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class DatasetInfo {

    private final AqlBinaryComparatorFactoryProvider binaryComparatorFactoryProvider = AqlBinaryComparatorFactoryProvider.INSTANCE;
    private final AqlNormalizedKeyComputerFactoryProvider normalizedKeyComputerProvider = AqlNormalizedKeyComputerFactoryProvider.INSTANCE;

    private final String[] locationConstraints;
    private final IFileSplitProvider fileSplitProvider;
    private final ARecordType recordType;
    private final int[] sortFields;
    private final int[] fieldPermutation;

    private final IBinaryComparatorFactory[] primaryKeyBinaryComparatorFactories;
    private final INormalizedKeyComputerFactory primaryKeyNormalizedKeyComputerFactory;
    private final RecordDescriptor recordDescriptor;

    @SuppressWarnings("rawtypes")
    public DatasetInfo(String[] locationConstraints, IFileSplitProvider fileSplitProvider, ARecordType recordType,
            String[] primaryKeyFields) throws Exception {
        this.locationConstraints = locationConstraints;
        this.fileSplitProvider = fileSplitProvider;
        this.recordType = recordType;
        this.primaryKeyBinaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFields.length];

        // Initializes comparator factories and normalized key.
        for (int i = 0; i < primaryKeyFields.length; i++) {
            String fieldName = primaryKeyFields[i];
            ATypeTag fieldTypeTag = recordType.getFieldType(fieldName).getTypeTag();
            primaryKeyBinaryComparatorFactories[i] = binaryComparatorFactoryProvider.getBinaryComparatorFactory(
                    fieldTypeTag, true);
        }
        ATypeTag fieldTypeTag = recordType.getFieldType(primaryKeyFields[0]).getTypeTag();
        primaryKeyNormalizedKeyComputerFactory = normalizedKeyComputerProvider.getNormalizedKeyComputerFactory(
                fieldTypeTag, true);
        // Initializes sort fields.
        sortFields = new int[primaryKeyFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            sortFields[i] = i;
        }
        // Initializes field permutations.
        fieldPermutation = new int[primaryKeyFields.length + 1];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        //Initializes record descriptor.
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[primaryKeyFields.length + 1];
        ITypeTraits[] typeTraits = new ITypeTraits[primaryKeyFields.length + 1];
        for (int i = 0; i < typeTraits.length; i++) {
            typeTraits[i] = new TypeTraits(false);
        }
        this.recordDescriptor = new RecordDescriptor(serdes, typeTraits);
    }

    public String[] getLocationConstraints() {
        return locationConstraints;
    }

    public IFileSplitProvider getFileSplitProvider() {
        return fileSplitProvider;
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public IBinaryComparatorFactory[] getPrimaryKeyComparatorFactories() {
        return primaryKeyBinaryComparatorFactories;
    }

    public INormalizedKeyComputerFactory getPrimaryKeyNormalizedKeyComputerFactory() {
        return primaryKeyNormalizedKeyComputerFactory;
    }

    public int[] getSortFields() {
        return sortFields;
    }

    public int[] getFieldPermutation() {
        return fieldPermutation;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

}
