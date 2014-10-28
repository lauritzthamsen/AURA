package de.tuberlin.aura.core.iosystem;

import org.objenesis.instantiator.ObjectInstantiator;

public class ReusingObjectInstantiator<T> implements ObjectInstantiator<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    T instance;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ReusingObjectInstantiator(ObjectInstantiator<T> baseInstanstiator) {
        instance = baseInstanstiator.newInstance();
    }

    // ---------------------------------------------------
    // Public methods.
    // ---------------------------------------------------

    @Override
    public T newInstance() {
        return instance;
    }

}
