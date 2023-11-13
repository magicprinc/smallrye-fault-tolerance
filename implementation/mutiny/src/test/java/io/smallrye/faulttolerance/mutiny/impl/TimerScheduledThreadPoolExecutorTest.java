package io.smallrye.faulttolerance.mutiny.impl;

import io.smallrye.faulttolerance.core.timer.ThreadTimer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 @see TimerScheduledThreadPoolExecutor
*/
class TimerScheduledThreadPoolExecutorTest {

    static final Executor EXECUTOR = Runnable::run;
    static final ThreadTimer TIMER = ThreadTimer.create(EXECUTOR);


    @AfterAll
    static void afterAll () throws InterruptedException{
        TIMER.shutdown();
    }


    @Test void basic () throws Exception {
        ScheduledExecutorService ses = new TimerScheduledThreadPoolExecutor(TIMER, EXECUTOR);

        AtomicInteger cnt = new AtomicInteger();

        ses.execute(cnt::incrementAndGet);
        assertEquals(1, cnt.get());

        ses.submit(cnt::incrementAndGet);
        assertEquals(2, cnt.get());

        ses.submit(cnt::incrementAndGet, 42);
        assertEquals(3, cnt.get());

        ses.submit((Runnable) cnt::incrementAndGet);
        assertEquals(4, cnt.get());

        //
        {
            cnt.set(0);
            var f = ses.schedule((Runnable) cnt::incrementAndGet, 100, null);
            assertEquals(0, cnt.get());
            assertFalse(f.isDone());
            assertNull(f.get());// wait
            assertEquals(1, cnt.get());
            assertTrue(f.isDone());
            assertFalse(f.isCancelled());
        }
        {
            cnt.set(0);
            var f = ses.schedule((Runnable) cnt::incrementAndGet, 100, null);
            assertEquals(0, cnt.get());
            assertFalse(f.isDone());
            assertTrue(f.cancel(true));
            assertEquals(0, cnt.get());
            assertTrue(f.isDone());
            assertTrue(f.isCancelled());
            assertThrows(CancellationException.class, f::get);
        }

        {
            cnt.set(0);
            var f = ses.schedule(cnt::incrementAndGet, 100, null);
            assertEquals(0, cnt.get());
            assertFalse(f.isDone());
            assertEquals(1, f.get());// wait
            assertEquals(1, cnt.get());
            assertTrue(f.isDone());
            assertFalse(f.isCancelled());
        }
        {
            cnt.set(0);
            var f = ses.schedule(cnt::incrementAndGet, 100, null);
            assertEquals(0, cnt.get());
            assertFalse(f.isDone());
            assertTrue(f.cancel(true));
            assertEquals(0, cnt.get());
            assertTrue(f.isDone());
            assertTrue(f.isCancelled());
            assertThrows(CancellationException.class, f::get);
        }

        {
            cnt.set(0);
            var f = ses.schedule(()->{
                    cnt.incrementAndGet();
                    throw new IOException();
                }, 100, null);
            assertEquals(0, cnt.get());
            assertFalse(f.isDone());
            assertThrows(ExecutionException.class, f::get);// wait
            assertEquals(1, cnt.get());
            assertTrue(f.isDone());
            assertFalse(f.isCancelled());
        }
    }

    @Test void _scheduleAtFixedRate () throws InterruptedException{
        ScheduledExecutorService ses = new TimerScheduledThreadPoolExecutor(TIMER, EXECUTOR);

        CountDownLatch cnt = new CountDownLatch(5);

        var runs = new ConcurrentHashMap<Long,Long>();
        long start = System.nanoTime();

        ScheduledFuture<?> f = ses.scheduleAtFixedRate(()->{
            runs.put(5-cnt.getCount(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            cnt.countDown();
        }, 10, 20, TimeUnit.MILLISECONDS);

        cnt.await();
        assertEquals(5, runs.size());
        System.out.println("scheduleAtFixedRate: "+runs);
    }

    @Test void _scheduleWithFixedDelay () throws InterruptedException{
        ScheduledExecutorService ses = new TimerScheduledThreadPoolExecutor(TIMER, EXECUTOR);

        CountDownLatch cnt = new CountDownLatch(5);

        var runs = new ConcurrentHashMap<Long,Long>();
        long start = System.nanoTime();

        ScheduledFuture<?> f = ses.scheduleWithFixedDelay(()->{
            runs.put(5-cnt.getCount(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            cnt.countDown();
        }, 10, 20, TimeUnit.MILLISECONDS);

        cnt.await();
        assertEquals(5, runs.size());
        System.out.println("scheduleWithFixedDelay: "+runs);
    }
}
