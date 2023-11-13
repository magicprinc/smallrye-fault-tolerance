package io.smallrye.faulttolerance.core.timer;

/**
 @see TimerTask
 @see ThreadTimer.Task
 */
public interface TimerTaskSpi {

    TimerTask reschedule (long delayInMillis, Runnable command);

}
