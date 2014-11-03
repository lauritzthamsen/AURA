package de.tuberlin.aura.core.dataflow.operators.impl;


import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.OperatorResult;

import java.util.Arrays;


public class LoopControlPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean loopTerminated;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LoopControlPhysicalOperator(final IExecutionContext context,
                                       final IPhysicalOperator<I> inputOp) {

        super(context, inputOp);

        this.loopTerminated = false;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        inputOp.open();
    }

    @Override
    public OperatorResult<I> next() throws Throwable {

        if (loopTerminated) {
            setOutputGates(Arrays.asList(0));
        } else {
            setOutputGates(Arrays.asList(1));
        }

        return inputOp.next();
    }

    @Override
    public void close() throws Throwable {
        super.close();
        inputOp.close();
    }
    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}

