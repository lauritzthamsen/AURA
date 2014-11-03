package de.tuberlin.aura.core.dataflow.operators.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.in.CSVInputFormat;
import de.tuberlin.aura.core.filesystem.in.InputFormat;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public class HDFSSourcePhysicalOperator<O> extends AbstractUnaryPhysicalOperator<Object,O> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String HDFS_SOURCE_FILE_PATH = "HDFS_SOURCE_FILE_PATH";

    public static final String HDFS_SOURCE_INPUT_FIELD_TYPES = "HDFS_SOURCE_INPUT_FIELD_TYPES";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private FileInputSplit split;

    private InputFormat<AbstractTuple, FileInputSplit> inputFormat;

    private AbstractTuple record;

    private OperatorResult<O> operatorResultInstance;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HDFSSourcePhysicalOperator(final IExecutionContext context) {

        super(context, null);

        operatorResultInstance = new OperatorResult<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {

        super.open();

        final Path path = new Path((String)getContext().getProperties().config.get(HDFS_SOURCE_FILE_PATH));
        final Class<?>[] fieldTypes = (Class<?>[]) getContext().getProperties().config.get(HDFS_SOURCE_INPUT_FIELD_TYPES);

        inputFormat = new CSVInputFormat(path, fieldTypes);
        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", getContext().getRuntime().getTaskManager().getConfig().getString("tm.io.hdfs.hdfs_url"));
        inputFormat.configure(conf);

        split = (FileInputSplit)getContext().getRuntime().getNextInputSplit();

        record = AbstractTuple.createTuple(((CSVInputFormat<AbstractTuple>)inputFormat).getFieldTypes().length);

        inputFormat.open(split);
    }

    @Override
    public OperatorResult<O> next() throws Throwable {

        inputFormat.nextRecord(record);

        if (inputFormat.reachedEnd()) {
            inputFormat.close();
            split = (FileInputSplit)getContext().getRuntime().getNextInputSplit();

            if (split == null) {
                return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
            }

            inputFormat.open(split);
            inputFormat.nextRecord(record);
        }

        operatorResultInstance.element = (O) record;

        return operatorResultInstance;
    }

    @Override
    public void close() throws Throwable {
        super.close();
        inputFormat.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
