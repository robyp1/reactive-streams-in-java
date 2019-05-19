package forkjoinexAndRxExamples.FileListStrategies;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Stream;

public class SizeOfFileTaskDelegate extends RecursiveTask<Long> {


    private static final long serialVersionUID = -4262203394349349317L;
    private File file;
    private Boolean debug;

    public SizeOfFileTaskDelegate() {
    }

    public SizeOfFileTaskDelegate(Boolean debug, File file) {
        if (debug) System.out.println( "constructor thread " + Thread.currentThread().getName());
        this.file = Objects.requireNonNull(file);
        this.debug = debug;
    }

    @Override
    protected Long compute() {
        Long size = 0L;
        if (debug) System.out.println( "begin compute with thread " + Thread.currentThread().getName());
        if (file.isFile()){
            return file.length();
        }
        else {
            final List<SizeOfFileTaskDelegate> recursiveTaskList= new ArrayList<>();
            final Stream<File> files = Stream.of(file.listFiles());

            //JDK 8+
            files.forEach( child -> {
                final SizeOfFileTaskDelegate task = new SizeOfFileTaskDelegate(debug, child);
                task.fork();
                recursiveTaskList.add(task);
                if (debug) System.out.println( "process compute" + child.getAbsolutePath() +" with thread " + Thread.currentThread().getName());
            });
            size = recursiveTaskList.stream().mapToLong(task -> task.join()).sum();

//                final File[] children = fileRoot.listFiles(); //JDK 7
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



    public void countFileInaDirParallel() {
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        Long sizeOfDir = 0l;
        long start = System.currentTimeMillis();
        try {
            sizeOfDir = forkJoinPool.invoke(this);
            System.out.println("*With fork join pool* size in byte of " + file.getAbsolutePath() +  ": " + sizeOfDir);
            System.out.println("*With fork join pool* Elapsed " + (System.currentTimeMillis() - start));
        } finally {
            forkJoinPool.shutdown();
        }

    }
}