package org.apache.hyracks.imru.dataflow;

import java.io.Serializable;

import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.imru.api.IIMRUJob2;
import org.apache.hyracks.imru.file.ConfigurationFactory;

abstract public class IMRUOperatorDescriptor<Model extends Serializable, Data extends Serializable>
        extends AbstractSingleActivityOperatorDescriptor {
    protected final IIMRUJob2<Model, Data> imruSpec;

    public IMRUOperatorDescriptor(IOperatorDescriptorRegistry spec,
            int inputArity, int outputArity, String name,
            IIMRUJob2<Model, Data> imruSpec) {
        super(spec, inputArity, outputArity);
        this.setDisplayName(name);
        this.imruSpec = imruSpec;
    }
}
