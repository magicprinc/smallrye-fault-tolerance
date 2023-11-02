package io.smallrye.faulttolerance.standalone;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

import io.smallrye.faulttolerance.core.timer.ThreadTimer;
import io.smallrye.faulttolerance.core.timer.Timer;

/**
 * Settings of {@link StandaloneFaultToleranceSpi} (instead of complex config files and obscure system properties).
 * Must be set before any access to any other class in the Fault Tolerance library.
 */
public final class Settings {
    public static Supplier<ExecutorService> createExecutorService = Executors::newCachedThreadPool;
    public static Function<Executor, Timer> createTimer = ThreadTimer::new;
}
