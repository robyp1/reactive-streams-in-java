package forkjoinex;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * esempio di Fork Join Pool (java 8+)
 * usato anche per i Reactive per consumare in parallelo gli elementi emessi dal publisher
 * Questo esempio scorre i file e conta la dimensione di una directory
 */
public class AsyncFileList
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFileList.class);
    static Boolean debug = true;

    /**
     * versione con forkjoin pool
     * time elapsed for 1 subdir and 12 files: 31ms
     */
    public static class SizeOfFileTask extends RecursiveTask<Long> {

        private final File file;
        private final Boolean debug;

        public SizeOfFileTask(File file, Boolean debug) {
            if (debug) System.out.println( "constructor thread " + Thread.currentThread().getName());
            this.file = Objects.requireNonNull(file);
            this.debug = debug;
        }

        private static final long serialVersionUID = -4262203394349349317L;

        @Override
        protected Long compute() {
            Long size = 0L;
            if (debug) System.out.println( "begin compute with thread " + Thread.currentThread().getName());
            if (file.isFile()){
                return file.length();
            }
            else {
                final List<SizeOfFileTask> recursiveTaskList= new ArrayList<>();
                final Stream<File> files = Stream.of(file.listFiles());

                //JDK 8+
                files.forEach( child -> {
                            final SizeOfFileTask task = new SizeOfFileTask(child, debug);
                            task.fork();
                            recursiveTaskList.add(task);
                            if (debug) System.out.println( "process compute" + child.getAbsolutePath() +" with thread " + Thread.currentThread().getName());
                            });
                size = recursiveTaskList.stream().mapToLong(task -> task.join()).sum();

//                final File[] children = file.listFiles(); //JDK 7
//                if (children != null) {
//                    for (final File child : children) {
//                        final SizeOfFileTask task = new SizeOfFileTask(child);
//                        task.fork();
//                        recursiveTaskList.add(task);
//                    }
//                }
//                for (final SizeOfFileTask task : recursiveTaskList) { //JDK 7
//                    size += task.join();
//                }

            }
            return size;
        }
    }


    /**
     * versione con rxjava 2
     * time elapsed for 1 subdir and 12 files: 125ms
     */
    final static BlockingQueue<Long> sizeList = new ArrayBlockingQueue<Long>(100);
    final static BlockingQueue th = new ArrayBlockingQueue<String>(30);

    public static void countFileInaDirParallel(File directory) {
        Flowable.just(directory)
                .flatMap(dir ->
                        Flowable.fromArray(dir.listFiles()))
                .filter(File::isFile)
                .flatMap(file ->
                        Flowable.just(file)
                                .subscribeOn(Schedulers.io()) // execute on multiple threads
                                .map(fs -> readFileFunctionWithReturn(fs))
                                .doOnNext(f->th.add(file.getName()))
                )
                .subscribeOn(Schedulers.io())//cambia il thread principale da main a io (sale su, upstream)
                .observeOn(Schedulers.computation()) // observe on UI, or choose another Scheduler, va giù (downstream) e add in list gestita da questo thread
                .blockingSubscribe(fileResult -> { //termina quando l'array è completato
                    if (debug) System.out.println("consume item " + th.toString() + " last size:" + fileResult);
                    // unordered results
                    sizeList.add(fileResult);
                });
    }

    private  static Long readFileFunctionWithReturn(File file) {
        if (debug)
            System.out.println("size in byte of " + file.getAbsolutePath() +  ": " + file.length());
        return Long.valueOf(file.length());
    }

    private static void countingAllFiles(File file) throws InterruptedException, ExecutionException {
        Long size = 0l;
        Long sizeTot = 0l;
        if (file.isDirectory()) {
            countFileInaDirParallel(file);
            List<File> childs = Stream.of(file.listFiles()).collect(Collectors.toList());
            for (File child :childs) {
                if (child.isDirectory()) {
                    countFileInaDirParallel(child);
                }
            }
        }
    }



    public static void main (String[] args) throws ExecutionException, InterruptedException {
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        debug = false;
        File file = new File("c:/Installativi");
        Long sizeOfDir = 0l;
        long start = System.currentTimeMillis();
        try {
            sizeOfDir = forkJoinPool.invoke(new SizeOfFileTask(file,debug));
            System.out.println("size in byte of " + file.getAbsolutePath() +  ": " + sizeOfDir);
            System.out.println("Elapsed " + (System.currentTimeMillis() - start));
        } finally {
            forkJoinPool.shutdown();
        }
        start = System.currentTimeMillis();
        countingAllFiles(file);
        long size = sizeList.stream().mapToLong(fsize -> fsize.longValue()).sum();
        System.out.println("size in byte of " + file.getAbsolutePath() + ": " + size);

        //Assert.isTrue(sizeOfDir.longValue()== size);
        System.out.println("Elapsed " + (System.currentTimeMillis() - start));

    }


}


