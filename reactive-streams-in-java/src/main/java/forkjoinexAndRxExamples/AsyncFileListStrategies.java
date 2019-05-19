package forkjoinexAndRxExamples;

import java.io.File;
import java.util.Objects;

public abstract class AsyncFileListStrategies {

    protected final File fileRoot;
    protected final Boolean debug;

    public AsyncFileListStrategies(Boolean debug, File fileRoot) {
        this.fileRoot = Objects.requireNonNull(fileRoot);
        this.debug = debug;
    }

    public abstract void countFileInaDirParallel() throws Exception;

    protected Long printSizeOf(File file) {
        if (debug)
            System.out.println("size in byte of " + file.getAbsolutePath() +  ": " + file.length());
        return Long.valueOf(file.length());
    }
}
