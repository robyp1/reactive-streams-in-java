package forkjoinexAndRxExamples.FileListStrategies;

import forkjoinexAndRxExamples.AsyncFileListStrategies;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * esempio di Fork Join Pool (java 8+)
 * usato anche per i Reactive per consumare in parallelo gli elementi emessi dal publisher
 * Questo esempio scorre i fileRoot e conta la dimensione di una directory
 */
public class AsyncFileListRxStrategy extends AsyncFileListStrategies {


    /**
     * versione con rxjava 2
     * time elapsed for 1 subdir and 12 files: 125ms
     */
    private final BlockingQueue<Long> sizeList = new ArrayBlockingQueue<Long>(100);
    private final BlockingQueue th = new ArrayBlockingQueue<String>(30);
    private final Logger logger = LoggerFactory.getLogger(AsyncFileListRxStrategy.class);

    public AsyncFileListRxStrategy(Boolean debug, File file) {
        super(debug, file);
    }


    @Override
    public void countFileInaDirParallel() throws Exception {
        long start  =System.currentTimeMillis();
        Long size = 0l;
        if (fileRoot.isDirectory()) {
            countSizeOfDir(fileRoot);
            List<File> childs = Stream.of(fileRoot.listFiles()).collect(Collectors.toList());
            for (File child :childs) {
                if (child.isDirectory()) {
                    countSizeOfDir(child);
                }
            }
        }

        size = sizeList.stream().mapToLong(fsize -> fsize.longValue()).sum();
        System.out.println("*With RX* size in byte of " + fileRoot.getAbsolutePath() + ": " + size);
        System.out.println("*With RX* Elapsed " + (System.currentTimeMillis() - start));
    }

    private void countSizeOfDir(File child) {
        if (child.isDirectory()) {
            Flowable.just(child)
                    .flatMap(dir ->
                            Flowable.fromArray(dir.listFiles()))
                    .filter(File::isFile)
                    .flatMap(filew ->
                            Flowable.just(filew)
                                    .subscribeOn(Schedulers.io()) // execute on multiple threads
                                    .map(fs -> printSizeOf(fs))
                                    .doOnNext(f->th.add(filew.getName()))
                    )
                    .subscribeOn(Schedulers.io())//cambia il thread principale da main a io (sale su, upstream)
                    .observeOn(Schedulers.computation()) // observe on UI, or choose another Scheduler, va giù (downstream) e add in list gestita da questo thread
                    .blockingSubscribe(fileResult -> { //termina quando l'array è completato
                        if (super.debug) System.out.println("consume item " + th.toString() + " last size:" + fileResult);
                        // unordered results
                        sizeList.add(fileResult);
                    });
        }
    }


}


