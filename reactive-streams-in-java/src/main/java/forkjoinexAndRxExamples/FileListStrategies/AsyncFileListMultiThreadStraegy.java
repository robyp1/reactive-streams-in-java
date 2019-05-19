package forkjoinexAndRxExamples.FileListStrategies;

import forkjoinexAndRxExamples.AsyncFileListStrategies;

import java.io.File;

/**
 * versione con forkjoin pool
 * time elapsed for 1 subdir and 12 files: 31ms
 * delegate action to class SizeOfFileTaskDelegate that use a ForkJoin Pool Threads
 * , available from jdk 1.8 to next versions
 */
public class AsyncFileListMultiThreadStraegy extends AsyncFileListStrategies {

    private final SizeOfFileTaskDelegate sizeOfFileTaskDelegate;

    public AsyncFileListMultiThreadStraegy(Boolean debug, File file) {
        super(debug, file);
        sizeOfFileTaskDelegate = new SizeOfFileTaskDelegate(debug, file);
    }

    @Override
    public void countFileInaDirParallel() throws Exception {

        sizeOfFileTaskDelegate.countFileInaDirParallel();
    }
}
