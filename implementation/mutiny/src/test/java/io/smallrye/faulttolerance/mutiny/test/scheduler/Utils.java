package io.smallrye.faulttolerance.mutiny.test.scheduler;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.*;

public final class Utils {
    public static final boolean testImplementationDetails = true;

    /**
     * Returns the value of 'test.timeout.factor' system property
     * converted to {@code double}.
     */
    public static final double TIMEOUT_FACTOR;
    static {
        String toFactor = System.getProperty("test.timeout.factor", "1.0");
        TIMEOUT_FACTOR = Double.parseDouble(toFactor);
    }

    /**
     * Returns the value of JTREG default test timeout in milliseconds
     * converted to {@code long}.
     */
    public static final long DEFAULT_TEST_TIMEOUT = TimeUnit.SECONDS.toMillis(120);

    // Delays for timing-dependent tests, in milliseconds.

    public static long SHORT_DELAY_MS;
    public static long SMALL_DELAY_MS;
    public static long MEDIUM_DELAY_MS;
    public static long LONG_DELAY_MS = adjustTimeout(10_000);

    /**
     * A delay significantly longer than LONG_DELAY_MS.
     * Use this in a thread that is waited for via awaitTermination(Thread).
     */
    public static long LONGER_DELAY_MS;

    /**
     * Adjusts the provided timeout value for the TIMEOUT_FACTOR
     * @param tOut the timeout value to be adjusted
     * @return The timeout value adjusted for the value of "test.timeout.factor"
     *         system property
     */
    public static long adjustTimeout(long tOut) {
        return Math.round(tOut * TIMEOUT_FACTOR);
    }

    /**
     * The scaling factor to apply to standard delays used in tests.
     * May be initialized from any of:
     * - the "jsr166.delay.factor" system property
     * - the "test.timeout.factor" system property (as used by jtreg)
     *   See: https://openjdk.org/jtreg/tag-spec.html
     * - hard-coded fuzz factor when using a known slowpoke VM
     */
    public static final float delayFactor = delayFactor();

    private static float delayFactor() {
        float x;
        if (!Float.isNaN(x = systemPropertyValue("jsr166.delay.factor")))
            return x;
        if (!Float.isNaN(x = systemPropertyValue("test.timeout.factor")))
            return x;
        String prop = System.getProperty("java.vm.version");
        if (prop != null && prop.matches(".*debug.*"))
            return 4.0f; // How much slower is fastdebug than product?!
        return 1.0f;
    }

    /**
     * Returns the value of the system property, or NaN if not defined.
     */
    private static float systemPropertyValue(String name) {
        String floatString = System.getProperty(name);
        if (floatString == null)
            return Float.NaN;
        try {
            return Float.parseFloat(floatString);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                String.format("Bad float value in system property %s=%s",
                    name, floatString));
        }
    }

    private static final long RANDOM_TIMEOUT;
    private static final long RANDOM_EXPIRED_TIMEOUT;
    private static final TimeUnit RANDOM_TIMEUNIT;
    static {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long[] timeouts = { Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE };
        RANDOM_TIMEOUT = timeouts[rnd.nextInt(timeouts.length)];
        RANDOM_EXPIRED_TIMEOUT = timeouts[rnd.nextInt(3)];
        TimeUnit[] timeUnits = TimeUnit.values();
        RANDOM_TIMEUNIT = timeUnits[rnd.nextInt(timeUnits.length)];
    }

    /**
     * Returns a timeout for use when any value at all will do.
     */
    public static long randomTimeout() { return RANDOM_TIMEOUT; }

    /**
     * Returns a timeout that means "no waiting", i.e. not positive.
     */
    public static long randomExpiredTimeout() { return RANDOM_EXPIRED_TIMEOUT; }

    /**
     * Returns a random non-null TimeUnit.
     */
    public static TimeUnit randomTimeUnit() { return RANDOM_TIMEUNIT; }

    /**
     * Returns a random boolean; a "coin flip".
     */
    public static boolean randomBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }
    /**
     * Fails with message "should throw exception".
     */
    public static void shouldThrow() {
        fail("Should throw exception");
    }

    /**
     * Fails with message "should throw " + exceptionName.
     */
    public static void shouldThrow(String exceptionName) {
        fail("Should throw " + exceptionName);
    }

    public static Runnable possiblyInterruptedRunnable (long timeoutMillis) {
        return (CheckedRunnable) ()->{
						try {
								delay(timeoutMillis);
						} catch (InterruptedException ok) {}
				};
    }

    /**
     * Delays, via Thread.sleep, for the given millisecond delay, but
     * if the sleep is shorter than specified, may re-sleep or yield
     * until time elapses.  Ensures that the given time, as measured
     * by System.nanoTime(), has elapsed.
     */
    static void delay(long millis) throws InterruptedException {
        long nanos = millis * (1000 * 1000);
        final long wakeupTime = System.nanoTime() + nanos;
        do {
            if (millis > 0L)
                Thread.sleep(millis);
            else // too short to sleep
                Thread.yield();
            nanos = wakeupTime - System.nanoTime();
            millis = nanos / (1000 * 1000);
        } while (nanos >= 0L);
    }

    /**
     * Returns the number of milliseconds since time given by
     * startNanoTime, which must have been previously returned from a
     * call to {@link System#nanoTime()}.
     */
    public static long millisElapsedSince(long startNanoTime) {
        return NANOSECONDS.toMillis(System.nanoTime() - startNanoTime);
    }
}
