package io.smallrye.faulttolerance.mutiny.test.scheduler;

@FunctionalInterface
public interface CheckedRunnable extends Runnable {

    void realRun () throws Throwable;


    @Override default void run (){
        try {
            realRun();
        } catch (Throwable e){
            throw new AssertionError(e);
        }
    }
}
