package io.smallrye.faulttolerance.api;

import io.smallrye.common.annotation.Experimental;

import java.util.function.Function;

/**
 * This is an internal API. It may change incompatibly without notice.
 * It should not be used or implemented outside SmallRye Fault Tolerance.
 */
@Experimental("first attempt at providing programmatic API")
public interface FaultToleranceSpi {
    boolean applies();

    int priority();

    <T, R> FaultTolerance.Builder<T, R> newBuilder(Function<FaultTolerance<T>, R> finisher);

    <T, R> FaultTolerance.Builder<T, R> newAsyncBuilder(Class<?> asyncType, Function<FaultTolerance<T>, R> finisher);

    CircuitBreakerMaintenance circuitBreakerMaintenance();

    TimerAccess timerAccess();
}
