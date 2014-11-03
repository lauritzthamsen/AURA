package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.ISourceFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public class UDFSourcePhysicalOperator<O> extends AbstractUnaryUDFPhysicalOperator<Object,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private OperatorResult<O> operatorResultInstance;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UDFSourcePhysicalOperator(final IExecutionContext context,
                                     final SourceFunction<O> function) {

        super(context, null, function);

        operatorResultInstance = new OperatorResult<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
    }

    @Override
    public OperatorResult<O> next() throws Throwable {

        O element = ((ISourceFunction<O>)function).produce();

        if (element == null) {
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
        }

        operatorResultInstance.element = element;

        return operatorResultInstance;
    }

    @Override
    public void close() throws Throwable {
        super.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
