package io.smallrye.faulttolerance.core.timer;

import io.smallrye.faulttolerance.core.util.RunnableWrapper;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static io.smallrye.faulttolerance.core.timer.TimerLogger.LOG;
import static io.smallrye.faulttolerance.core.util.Preconditions.checkNotNull;

/**
 * Starts one thread that processes submitted tasks in a loop and when it's time for a task to run,
 * it gets submitted to the executor. The default executor is provided by a caller, so the caller
 * must shut down this timer <em>before</em> shutting down the executor.
 */
// TODO implement a hashed wheel?
public final class ThreadTimer implements Timer {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    private static final Set<ThreadTimer> TIMERS = ConcurrentHashMap.newKeySet();

    private static final Comparator<Task> TASK_COMPARATOR = (o1, o2) -> {
        if (o1 == o2) {
            // two different instances are never equal
            return 0;
        }

        // must _not_ return 0 if start times are equal, because that isn't consistent
        // with `equals` (see also above)
        return o1.startTime < o2.startTime ? -1
            : o1.startTime > o2.startTime ? 1
            : o1.hashCode() <= o2.hashCode() ? -1 : 1;
    };

    private final String name;

    final PQueue tasks = new PQueue();

    private final Thread thread;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public final Executor defaultExecutor;

    /**
     * @param defaultExecutor default {@link Executor} used for running scheduled tasks, unless an executor
     *        is provided when {@linkplain #schedule(long, Runnable, Executor) scheduling} a task
     */
    public ThreadTimer(Executor defaultExecutor) {
        this.defaultExecutor = checkNotNull(defaultExecutor, "Executor must be set");

        this.name = "SmallRye Fault Tolerance Timer " + COUNTER.incrementAndGet();
        LOG.createdTimer(name);

        this.thread = new Thread(() -> {
            while (running.get()) {
                try {
                    if (tasks.isEmpty()) {
                        LockSupport.park();
                    } else {
                        Task task = tasks.peek();
                        if (task == null) continue;

                        long currentTime = System.nanoTime();
                        long taskStartTime = task.startTime;

                        // must _not_ use `taskStartTime <= currentTime`, because `System.nanoTime()`
                        // is relative to an arbitrary number and so it can possibly overflow;
                        // in such case, `taskStartTime` can be positive, `currentTime` can be negative,
                        //  and yet `taskStartTime` is _before_ `currentTime`
                        if (taskStartTime - currentTime <= 0) {
                            tasks.remove(task);
                            if (STATE.compareAndSet(task, Task.STATE_NEW, Task.STATE_RUNNING)) {
                                Executor executorForTask = task.executorOverride();
                                if (executorForTask == null) {
                                    executorForTask = defaultExecutor;
                                }

                                executorForTask.execute(task);
                            }
                        } else {
                            // this is OK even if another timer is scheduled during the sleep (even if that timer should
                            // fire sooner than `taskStartTime`), because `schedule` always calls` LockSupport.unpark`
                            LockSupport.parkNanos(taskStartTime - currentTime);
                        }
                    }
                } catch (Throwable e) {
                    // can happen e.g. when the executor is shut down sooner than the timer
                    LOG.unexpectedExceptionInTimerLoop(e);
                }
            }
        }, name);
        thread.start();
        TIMERS.add(this);
    }

    @Override
    public TimerTask schedule(long delayInMillis, Runnable task) {
        return schedule(delayInMillis, task, null);
    }

    @Override
    public TimerTask schedule(long delayInMillis, Runnable task, Executor executor) {
        long startTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayInMillis);
        Task timerTask = executor == null || executor == defaultExecutor
            ? new Task(startTime, RunnableWrapper.INSTANCE.wrap(task))
            : new Task(startTime, RunnableWrapper.INSTANCE.wrap(task)){
                @Override Executor executorOverride (){
                    return executor;
                }
            };
        tasks.add(timerTask);
        LockSupport.unpark(thread);
        LOG.scheduledTimerTask(timerTask, delayInMillis);
        return timerTask;
    }

    @Override
    public void shutdown() throws InterruptedException {
        if (running.compareAndSet(true, false)) {
            LOG.shutdownTimer(name);
            thread.interrupt();
            thread.join();
        }
        TIMERS.remove(this);
    }

    private static class Task implements TimerTask, Runnable, Comparable<Task> {
        static final byte STATE_NEW = 0; // was scheduled, but isn't running yet
        static final byte STATE_RUNNING = 1; // running on the executor
        static final byte STATE_FINISHED = 2; // finished running
        static final byte STATE_CANCELLED = 3; // cancelled before it could be executed

        final long startTime; // in nanos, to be compared with System.nanoTime()
        final Runnable runnable;
        volatile byte state = STATE_NEW;
        volatile int heapIndex = -2;

        Task(long startTime, Runnable runnable) {
            this.startTime = startTime;
            this.runnable = checkNotNull(runnable, "Runnable task must be set");
        }

        Executor executorOverride() {
            return null; // may be null, which means that the timer's executor shall be used
        }

        @Override
        public boolean isDone() {
            byte s = this.state;
            return s == STATE_FINISHED || s == STATE_CANCELLED;
        }

        @Override
        public boolean cancel() {
            // can't cancel if it's already running
            if (STATE.compareAndSet(this, STATE_NEW, STATE_CANCELLED)) {
                LOG.cancelledTimerTask(this);
                for (ThreadTimer next : TIMERS){
                    next.tasks.remove(this);
                }
                return true;
            }
            return false;
        }

        @Override
        public void run() {
            LOG.runningTimerTask(this);
            try {
                runnable.run();
            } finally {
                STATE.setRelease(this, Task.STATE_FINISHED);
            }
        }

        @Override public String toString() {
            return "TTask:"+state+':'+runnable+'@'+startTime;
        }

        public int getHeapIndex (){
            return heapIndex;
        }

        public void setHeapIndex (int heapIndex){
            this.heapIndex = heapIndex;
        }

        @Override public int compareTo (Task o){
            return TASK_COMPARATOR.compare(this, o);
        }
    }

    public int size() {
        return tasks.size();
    }

    /** For metrics and standalone-shutdown */
    public static Set<ThreadTimer> getThreadTimerRegistry() {
        return TIMERS;
    }


    public static class PQueue {
        private static final int INITIAL_CAPACITY = 5393;
        private Task[] queue = new Task[INITIAL_CAPACITY];
        private final ReentrantLock lock = new ReentrantLock();
        private int size;

        /**
         * Sets f's heapIndex if it is a ScheduledFutureTask.
         */
        private static void setIndex (Task f, int idx){
            f.setHeapIndex(idx);
        }

        /**
         * Sifts element added at bottom up to its heap-ordered spot.
         * Call only when holding lock.
         */
        private void siftUp (int k, Task key){
            while (k > 0) {
                int parent = (k - 1) >>> 1;
                var e = queue[parent];
                if (key.compareTo(e) >= 0)
                    break;
                queue[k] = e;
                setIndex(e, k);
                k = parent;
            }
            queue[k] = key;
            setIndex(key, k);
        }

        /**
         * Sifts element added at top down to its heap-ordered spot.
         * Call only when holding lock.
         */
        private void siftDown (int k, Task key){
            int half = size >>> 1;
            while (k < half) {
                int child = (k << 1) + 1;
                var c = queue[child];
                int right = child + 1;
                if (right < size && c.compareTo(queue[right]) > 0)
                    c = queue[child = right];
                if (key.compareTo(c) <= 0)
                    break;
                queue[k] = c;
                setIndex(c, k);
                k = child;
            }
            queue[k] = key;
            setIndex(key, k);
        }

        /**
         * Resizes the heap array.  Call only when holding lock.
         */
        private void grow (){
            int oldCapacity = queue.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1); // grow 50%
            if (newCapacity < 0 || newCapacity > MAX_Q_SIZE) {// overflow
                newCapacity = MAX_Q_SIZE;
                new Exception("TaskQueue overflow!").printStackTrace();
            }
            queue = Arrays.copyOf(queue, newCapacity);
        }

        public static final int MAX_Q_SIZE = 2_000_000_000;


        /**
         * Finds index of given object, or -1 if absent.
         */
        private int indexOf (Task his){
            int i = his.getHeapIndex();
            // Sanity check; x could conceivably be a
            // ScheduledFutureTask from some other pool.
            if (i >= 0 && i < size && queue[i] == his)
                return i;
            return -1;
        }

        public boolean contains (Task x){
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return indexOf(x) >= 0;
            } finally {
                lock.unlock();
            }
        }

        public boolean remove (Task x){
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int i = indexOf(x);
                if (i < 0)
                    return false;

                setIndex(queue[i], -1);
                int s = --size;
                var replacement = queue[s];
                queue[s] = null;
                if (s != i) {
                    siftDown(i, replacement);
                    if (queue[i] == replacement)
                        siftUp(i, replacement);
                }
                return true;
            } finally {
                lock.unlock();
            }
        }

        public int size (){
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return size;
            } finally {
                lock.unlock();
            }
        }

        public Task peek (){
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return queue[0];
            } finally {
                lock.unlock();
            }
        }

        public boolean add (Task e){
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int i = size;
                if (i >= queue.length)
                    grow();
                size = i + 1;
                if (i == 0) {
                    queue[0] = e;
                    setIndex(e, 0);
                } else {
                    siftUp(i, e);
                }
            } finally {
                lock.unlock();
            }
            return true;
        }

        public boolean isEmpty (){
            return size() <= 0;
        }
    }//PQueue

    // VarHandle mechanics
    private static final VarHandle STATE;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(Task.class, "state", byte.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
