package forkjoinexAndRxExamples;

import forkjoinexAndRxExamples.FileListStrategies.AsyncFileListMultiThreadStraegy;
import forkjoinexAndRxExamples.FileListStrategies.AsyncFileListRxStrategy;

import java.io.File;

public class MainClass {


    public static void main (String[] args) throws Exception {
        boolean debugRxEx = false;
        boolean debugForkJoinEx = false;
        File file = new File("c:/Installativi");

        AsyncFileListRxStrategy asyncFileListRxStrategy = new AsyncFileListRxStrategy(debugRxEx, file);
        asyncFileListRxStrategy.countFileInaDirParallel();

        AsyncFileListMultiThreadStraegy asyncFileListMultiThreadStraegy = new AsyncFileListMultiThreadStraegy(debugForkJoinEx, file);
        asyncFileListMultiThreadStraegy.countFileInaDirParallel();

    }
}
