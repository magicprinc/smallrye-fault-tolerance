package io.smallrye.faulttolerance.core.timer;

/**
 * Provides read-only view into the SmallRye Fault Tolerance timer.
 * <p>
 * Implementations must be thread-safe.
 */
public interface TimerAccess {
    /**
     * Returns the number of tasks that are currently scheduled for execution by the timer.
     * Finished tasks and tasks that are already running are not included.
     *
     * @return the number of currently scheduled tasks
     */
    int countScheduledTasks();
}
