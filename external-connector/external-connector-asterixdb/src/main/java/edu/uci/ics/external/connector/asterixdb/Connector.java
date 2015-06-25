package edu.uci.ics.external.connector.asterixdb;

import java.util.List;

import org.apache.hadoop.fs.Path;

import edu.uci.ics.external.connector.api.IConnector;
import edu.uci.ics.external.connector.api.IFieldConverterFactory;
import edu.uci.ics.external.connector.api.IReadOperatorDescriptorFactory;
import edu.uci.ics.external.connector.api.IWriteOperatorDescriptorFactory;

public class Connector implements IConnector {

    @Override
    public List<Path> getPaths(String... name) {
        return null;
    }

    @Override
    public IReadOperatorDescriptorFactory getReaderFactory(List<Path> outputPaths) {
        return null;
    }

    @Override
    public IWriteOperatorDescriptorFactory getWriterFactory(List<Path> inputPaths) {
        return null;
    }

    @Override
    public IFieldConverterFactory getFieldConverterFactory() {
        return null;
    }

}
