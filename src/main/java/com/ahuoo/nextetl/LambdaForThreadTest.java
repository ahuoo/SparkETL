package com.ahuoo.nextetl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Java 8 Concurrency Tutorial: Threads and Executors
 * https://winterbe.com/posts/2015/04/07/java8-concurrency-tutorial-thread-executor-examples/
 */
public class LambdaForThreadTest {
    public static void main(String[] args) throws Exception{
/*        testRunnable();
        testCallable();
        testInvokeAll();
        testInvokeAny();*/
        testScheduledExecutorsDelay();
//        testScheduledExecutorsFixedTimeRate();
//        testScheduledExecutorsFixedDelay();
    }

    public static void  testRunnable(){
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Hello " + threadName);
        });
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        }
        finally {
            if (!executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            executor.shutdownNow();
            System.out.println("shutdown finished");
        }
    }

    public static void testCallable() throws Exception{
        Callable<Integer> task = () -> {
            try {
                TimeUnit.SECONDS.sleep(3);
                return 123;
            }
            catch (InterruptedException e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Integer> future = executor.submit(task);
        System.out.println("future done? " + future.isDone());
        Integer result = future.get(10, TimeUnit.SECONDS);
        System.out.println("future done? " + future.isDone());
        System.out.println("result: " + result);
        executor.shutdown();
    }

    public static void testInvokeAll()  throws Exception{
        ExecutorService executor = Executors.newWorkStealingPool();

        List<Callable<String>> callables = Arrays.asList(
                () -> "task1",
                () -> "task2",
                () -> "task3");

        executor.invokeAll(callables)
                .stream()
                .map(future -> {
                    try {
                        return future.get();
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .forEach(System.out::println);
    }

    public static void testInvokeAny() throws Exception{

        ExecutorService executor = Executors.newWorkStealingPool();

        List<Callable<String>> callables = Arrays.asList(
                callable("task1", 2),
                callable("task2", 1),
                callable("task3", 3));

        String result = executor.invokeAny(callables);
        System.out.println(result);
        // => task2
    }

    private static Callable<String> callable(String result, long sleepSeconds) {
        return () -> {
            TimeUnit.SECONDS.sleep(sleepSeconds);
            return result;
        };
    }

    //This code sample schedules a task to run after an initial delay of three seconds has passed:
    public static void testScheduledExecutorsDelay() throws Exception{
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        Runnable task = () -> System.out.println("Scheduling: " + System.nanoTime());
        ScheduledFuture<?> future = executor.schedule(task, 3, TimeUnit.SECONDS);

        TimeUnit.MILLISECONDS.sleep(1000);

        long remainingDelay = future.getDelay(TimeUnit.MILLISECONDS);
        System.out.println("");
        System.out.printf("Remaining Delay: %sms", remainingDelay);
        System.out.println("");
    }

    /**
     * executing tasks with a fixed time rate, e.g. once every second as demonstrated in this example:
     * Please keep in mind that scheduleAtFixedRate() doesn't take into account the actual duration of the task.
     * So if you specify a period of one second but the task needs 2 seconds to be executed then the thread pool will working to capacity very soon.
     */
    public static void testScheduledExecutorsFixedTimeRate() throws Exception{
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                System.out.println("Scheduling: " + System.currentTimeMillis()/1000);
            }
            catch (InterruptedException e) {
                System.err.println("task interrupted");
            }
        };

        int initialDelay = 0;
        int period = 1;
        executor.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.SECONDS);
    }

    /**
     * In that case you should consider using scheduleWithFixedDelay() instead.
     * This method works just like the counterpart described above.
     * The difference is that the wait time period applies between the end of a task and the start of the next task. For example:
     *
     * This example schedules a task with a fixed delay of one second between the end of an execution and the start of the next execution.
     * The initial delay is zero and the tasks duration is two seconds. So we end up with an execution interval of 0s, 3s, 6s, 9s and so on
     */
    public static void testScheduledExecutorsFixedDelay() throws Exception{
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                System.out.println("Scheduling: " + System.currentTimeMillis()/1000);
            }
            catch (InterruptedException e) {
                System.err.println("task interrupted");
            }
        };

        executor.scheduleWithFixedDelay(task, 0, 1, TimeUnit.SECONDS);

    }
}
