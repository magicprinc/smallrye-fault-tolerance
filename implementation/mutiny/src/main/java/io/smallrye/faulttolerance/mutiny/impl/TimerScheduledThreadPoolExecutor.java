package io.smallrye.faulttolerance.mutiny.impl;

import io.smallrye.faulttolerance.core.timer.ThreadTimer;
import io.smallrye.faulttolerance.core.timer.Timer;
import io.smallrye.faulttolerance.core.timer.TimerTask;
import io.smallrye.faulttolerance.core.timer.TimerTaskSpi;
import io.smallrye.faulttolerance.core.util.SneakyThrow;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 Memory efficient {@link ScheduledExecutorService} based on {@link ThreadTimer}
 to replace {@link io.smallrye.mutiny.infrastructure.MutinyScheduler}.

 @see io.smallrye.mutiny.infrastructure.MutinyScheduler
 @see io.smallrye.faulttolerance.core.timer.Timer
 @see ThreadTimer
*/
public class TimerScheduledThreadPoolExecutor implements ScheduledExecutorService {

    private final Timer timer;
    private final ExecutorService executor;

    public TimerScheduledThreadPoolExecutor (Timer timer, Executor executor){
        this.timer = timer;
        this.executor = executor instanceof ExecutorService
            ? (ExecutorService) executor
            : new ExecutorServiceWrapper(executor);
    }//new


    @Override
    public void shutdown (){
        executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow(){
        return executor.shutdownNow();
    }

    @Override
    public boolean isShutdown(){
        return executor.isShutdown();
    }

    @Override
    public boolean isTerminated(){
        return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task){
        return executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result){
        return executor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task){
        return executor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return executor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return executor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
        return executor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute (Runnable command){
        executor.execute(command);
    }

    /**
     @see ScheduledThreadPoolExecutor.ScheduledFutureTask
     @see ThreadTimer.Task
     */
    abstract static class ComposedScheduledFutureBase<T> implements RunnableScheduledFuture<T> {
        final Object command;
        volatile TimerTask task;// has us as runnable

        public ComposedScheduledFutureBase (Runnable command){
            this.command = command;
        }//new

        public ComposedScheduledFutureBase (Callable<T> command){
            this.command = command;
        }//new

        @Override
        public final long getDelay (TimeUnit unit){
            throw new UnsupportedOperationException("TimerScheduledThreadPoolExecutor.ScheduledFuture.getDelay");
        }

        @Override
        public final int compareTo (Delayed other){
            throw new UnsupportedOperationException("TimerScheduledThreadPoolExecutor.ScheduledFuture.compareTo");
        }

        @Override
        public synchronized boolean cancel (boolean mayInterruptIfRunning){
            var t = task;
            if (t == null){
                return true; // already cancelled
            }
            if (t.isDone()){
                return false; // already done
            }

            boolean canceled = t.cancel();

            if (canceled){
                task = null;
            }
            return canceled;
        }

        @Override
        public boolean isCancelled (){
            return task == null;
        }

        @Override
        public boolean isDone (){
            var t = task;
            return t == null || t.isDone(); // canceled or done
        }
    }//ComposedScheduledFutureBase

    static class ComposedScheduledFuture<T> extends ComposedScheduledFutureBase<T> {
        volatile CompletableFuture<T> future;

        public ComposedScheduledFuture (Runnable command){
            super(command);
        }

        public ComposedScheduledFuture (Callable<T> command){
            super(command);
        }

        @Override
        public T get () throws InterruptedException, ExecutionException {
            if (isCancelled()){
                throw new CancellationException();
            }
            return getFuture().get();
        }

        @Override
        public T get (long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (isCancelled()){
                throw new CancellationException();
            }
            return getFuture().get(timeout, unit);
        }

        @Override
        public boolean isPeriodic (){
            return false;
        }

        private synchronized CompletableFuture<T> getFuture (){
            var f = future;
            if (f == null){
                f = new CompletableFuture<>();
                future = f;
            }
            return f;
        }

        @Override
        public void run (){
            if (isCancelled()){
                return;
            }
            try {
                if (command instanceof Runnable){
                    ((Runnable) command).run();
                    getFuture().complete(null);
                } else {
                    @SuppressWarnings("unchecked")
                    var result = ( (Callable<T>) command ).call();
                    getFuture().complete(result);
                }
            } catch (Throwable e){
                getFuture().completeExceptionally(e);
                SneakyThrow.sneakyThrow(e);
            }
        }

        @Override
        public boolean isDone (){
            if (super.isDone()){
                return true;
            }
            var f = future;
            if (f != null){
                return f.isDone();
            }
            return false;
        }
    }//ComposedScheduledFuture

    static class ComposedScheduledFuturePeriodic extends ComposedScheduledFutureBase<Void> {
        long period;

        public ComposedScheduledFuturePeriodic (Runnable command, long period){
            super(command);
            this.period = period;
        }//new

        @Override
        public Void get () throws InterruptedException, ExecutionException{
            return null;
        }

        @Override
        public Void get (long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        @Override
        public boolean isPeriodic (){
            return true;
        }

        @Override
        public void run (){
            if (isCancelled()){
                return;
            }
            try {
                ((Runnable) command).run();
            } catch (Throwable e){
                SneakyThrow.sneakyThrow(e);// logging in Task
            } finally {
                var t = task;
                if (t instanceof TimerTaskSpi){
                    task = ((TimerTaskSpi)t).reschedule(period, this);
                }
            }
        }
    }//ComposedScheduledFuturePeriodic


    @Override
    public ScheduledFuture<?> schedule (Runnable command, long delay, TimeUnit unit){
        var sf = new ComposedScheduledFuture<Void>(command);
		sf.task = timer.schedule(toMillis(delay, unit), sf, executor);
        return sf;
    }

    @Override
    public <V> ScheduledFuture<V> schedule (Callable<V> callable, long delay, TimeUnit unit){
        var sf = new ComposedScheduledFuture<V>(callable);
        sf.task = timer.schedule(toMillis(delay, unit), sf, executor);
        return sf;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate (Runnable command, long initialDelay, long period, TimeUnit unit){
        var sf = new ComposedScheduledFuturePeriodic(command, toPeriod(period, unit));
        sf.task = timer.schedule(toMillis(initialDelay, unit), sf, executor);
        return sf;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay (Runnable command, long initialDelay, long delay, TimeUnit unit){
        var sf = new ComposedScheduledFuturePeriodic(command, - toPeriod(delay, unit));
        sf.task = timer.schedule(toMillis(initialDelay, unit), sf, executor);
        return sf;
    }


    static long toMillis (long delay, TimeUnit unit){
        return unit != null
            ? unit.toMillis(delay)
            : delay;
    }

    static long toPeriod (long period, TimeUnit unit){
        if (unit == null){
            unit = TimeUnit.MILLISECONDS;
        }
        period = unit.toMillis((period < 0) ? 0 : period);

        if (period > MAX_PERIOD){
            throw new IllegalArgumentException("period/delay is too big: " + period + " " + unit);
        }

        return period;
    }
    private static final long MAX_PERIOD = Duration.ofDays(3660).toMillis();


    static final class ExecutorServiceWrapper extends AbstractExecutorService {
        private final Executor targetExecutor;

        public ExecutorServiceWrapper (Executor targetExecutor){
            this.targetExecutor = targetExecutor;
        }//new

        @Override public void execute (Runnable command) throws RejectedExecutionException {
            targetExecutor.execute(command);
        }

        @Override public void shutdown (){
        }

        @Override public List<Runnable> shutdownNow (){
            return Collections.emptyList();
        }

        @Override public boolean isShutdown (){
            return false;
        }

        @Override public boolean isTerminated (){
            return false;
        }

        @Override public boolean awaitTermination (long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }
    }//ExecutorServiceWrapper
}
