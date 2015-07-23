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
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlNormalizedKeyComputerFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class DatasetInfo {

    private final AqlBinaryComparatorFactoryProvider binaryComparatorFactoryProvider = AqlBinaryComparatorFactoryProvider.INSTANCE;
    private final AqlBinaryHashFunctionFactoryProvider binaryHashFunctionProvider = AqlBinaryHashFunctionFactoryProvider.INSTANCE;
    private final AqlNormalizedKeyComputerFactoryProvider normalizedKeyComputerProvider = AqlNormalizedKeyComputerFactoryProvider.INSTANCE;
    private final AqlTypeTraitProvider typeTraitProvider = AqlTypeTraitProvider.INSTANCE;
    private final AqlSerializerDeserializerProvider serdeProvider = AqlSerializerDeserializerProvider.INSTANCE;

    private final String[] locationConstraints;
    private final IFileSplitProvider fileSplitProvider;
    private final ARecordType recordType;
    private final boolean temp;
    private final int[] sortFields;
    private final int[] fieldPermutation;

    private final IBinaryHashFunctionFactory[] primaryKeyBinaryHashFunctionFactories;
    private final IBinaryComparatorFactory[] primaryKeyBinaryComparatorFactories;
    private final INormalizedKeyComputerFactory primaryKeyNormalizedKeyComputerFactory;
    private final RecordDescriptor recordDescriptor;

    @SuppressWarnings("rawtypes")
    public DatasetInfo(String[] locationConstraints, IFileSplitProvider fileSplitProvider, ARecordType recordType,
            String[] primaryKeyFields, boolean temp) throws Exception {
        this.locationConstraints = locationConstraints;
        this.fileSplitProvider = fileSplitProvider;
        this.recordType = recordType;
        this.temp = temp;
        this.primaryKeyBinaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFields.length];
        this.primaryKeyBinaryHashFunctionFactories = new IBinaryHashFunctionFactory[primaryKeyFields.length];

        // Initializes comparator factories and normalized key.
        for (int i = 0; i < primaryKeyFields.length; i++) {
            String fieldName = primaryKeyFields[i];
            IAType fieldType = recordType.getFieldType(fieldName);
            ATypeTag fieldTypeTag = fieldType.getTypeTag();
            primaryKeyBinaryComparatorFactories[i] = binaryComparatorFactoryProvider.getBinaryComparatorFactory(
                    fieldTypeTag, true);
            primaryKeyBinaryHashFunctionFactories[i] = binaryHashFunctionProvider
                    .getBinaryHashFunctionFactory(fieldType);
        }
        IAType fieldType = recordType.getFieldType(primaryKeyFields[0]);
        primaryKeyNormalizedKeyComputerFactory = normalizedKeyComputerProvider.getNormalizedKeyComputerFactory(
                fieldType, true);
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
        for (int i = 0; i < primaryKeyFields.length; i++) {
            IAType type = recordType.getFieldType(primaryKeyFields[i]);
            typeTraits[i] = typeTraitProvider.getTypeTrait(type);
            serdes[i] = serdeProvider.getNonTaggedSerializerDeserializer(type);
        }
        typeTraits[primaryKeyFields.length] = typeTraitProvider.getTypeTrait(recordType);
        serdes[primaryKeyFields.length] = serdeProvider.getNonTaggedSerializerDeserializer(recordType);
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

    public IBinaryHashFunctionFactory[] getPrimaryKeyHashFunctionFactories() {
        return primaryKeyBinaryHashFunctionFactories;
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

    public ITypeTraits[] getTypeTraits() {
        return recordDescriptor.getTypeTraits();
    }

    public boolean getTemp() {
        return temp;
    }

}
