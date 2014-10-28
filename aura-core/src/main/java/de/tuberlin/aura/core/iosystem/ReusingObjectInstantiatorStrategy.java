package de.tuberlin.aura.core.iosystem;

import java.util.List;

import org.objenesis.instantiator.ObjectInstantiator;
import org.objenesis.strategy.BaseInstantiatorStrategy;
import org.objenesis.strategy.InstantiatorStrategy;

import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;

public class ReusingObjectInstantiatorStrategy extends BaseInstantiatorStrategy {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private InstantiatorStrategy newInstanceStrategy;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ReusingObjectInstantiatorStrategy(InstantiatorStrategy baseStrategy) {
        this.newInstanceStrategy = baseStrategy;
    }

    // ---------------------------------------------------
    // Public methods.
    // ---------------------------------------------------

    @Override
    public <T> ObjectInstantiator<T> newInstantiatorOf(Class<T> type) {
        ObjectInstantiator<T> baseInstantiator = newInstanceStrategy.newInstantiatorOf(type);
        return new ReusingObjectInstantiator<>(baseInstantiator);
    }

    public static boolean isApplicable(List<DataflowNodeProperties> propertiesList) {

        boolean isApplicable = true;

        for (DataflowNodeProperties properties : propertiesList) {
            if (!isApplicableForOperatorType(properties.type)) {
                isApplicable = false;
            }
        }

        return isApplicable;
    }

    // ---------------------------------------------------
    // Private methods.
    // ---------------------------------------------------

    private static boolean isApplicableForOperatorType(DataflowNodeProperties.DataflowNodeType type) {

        // whitelist of operators for which instance reuse is an option (state-less + with inputs)

        switch (type) {
            case MAP_TUPLE_OPERATOR:
                return true;
            case FLAT_MAP_TUPLE_OPERATOR:
                return true;
            case UDF_SINK:
                return true;
            case HDFS_SINK:
                return true;
            case FILTER_OPERATOR:
                return true;
            case DATASET_UPDATE_OPERATOR:
                return true;
            case GROUP_BY_OPERATOR:
                return true;
            case HASH_FOLD_OPERATOR:
                return true;
            case LOOP_CONTROL_OPERATOR:
                return true;
            case UNION_OPERATOR:
                return true;
            default:
                return false;
        }
    }
}

