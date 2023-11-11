package io.smallrye.faulttolerance.mutiny.test.scheduler;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface CheckedCallable<T> extends Callable<T>  {

    @Override default T call () throws Exception {
        return realCall();
    }


    T realCall ();
}
