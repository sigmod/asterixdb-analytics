package edu.uci.ics.external.connector.asterixdb;

import edu.uci.ics.external.connector.api.IReadOperatorDescriptorFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

public class ReadOperatorDescriptorFactory implements IReadOperatorDescriptorFactory {

    @Override
    public IOperatorDescriptor getReaderOperatorDescriptor() {
        return null;
    }

    @Override
    public String[] getLocationConstraints() {
        return null;
    }

}
