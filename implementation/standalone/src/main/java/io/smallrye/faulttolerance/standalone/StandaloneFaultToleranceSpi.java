package io.smallrye.faulttolerance.standalone;

import io.smallrye.faulttolerance.api.CircuitBreakerMaintenance;
import io.smallrye.faulttolerance.api.FaultTolerance;
import io.smallrye.faulttolerance.api.FaultToleranceSpi;
import io.smallrye.faulttolerance.core.apiimpl.BuilderLazyDependencies;
import io.smallrye.faulttolerance.core.apiimpl.FaultToleranceImpl;
import io.smallrye.faulttolerance.core.timer.TimerAccess;

import java.util.function.Function;

public class StandaloneFaultToleranceSpi implements FaultToleranceSpi, TimerAccess {
    static class EagerDependenciesHolder {
        static final EagerDependencies INSTANCE = new EagerDependencies();
    }

    static class LazyDependenciesHolder {
        static final BuilderLazyDependencies INSTANCE = StandaloneFaultTolerance.getLazyDependencies();
    }

    @Override
    public boolean applies() {
        return true;
    }

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public <T, R> FaultTolerance.Builder<T, R> newBuilder(Function<FaultTolerance<T>, R> finisher) {
        return new FaultToleranceImpl.BuilderImpl<>(EagerDependenciesHolder.INSTANCE, () -> LazyDependenciesHolder.INSTANCE,
                null, finisher);
    }

    @Override
    public <T, R> FaultTolerance.Builder<T, R> newAsyncBuilder(Class<?> asyncType, Function<FaultTolerance<T>, R> finisher) {
        return new FaultToleranceImpl.BuilderImpl<>(EagerDependenciesHolder.INSTANCE, () -> LazyDependenciesHolder.INSTANCE,
                asyncType, finisher);
    }

    @Override
    public CircuitBreakerMaintenance circuitBreakerMaintenance() {
        return EagerDependenciesHolder.INSTANCE.cbMaintenance;
    }

    /**
     * Provides access to the timer that SmallRye Fault Tolernce internally uses for scheduling
     * purposes. It provides a read-only view into what the timer is doing.
     *
     * @return read-only view into the SmallRye Fault Tolerance timer
     */
    @Override
    public int countScheduledTasks (){
        return StandaloneFaultTolerance.getLazyDependencies().timer().countScheduledTasks();
    }
}
