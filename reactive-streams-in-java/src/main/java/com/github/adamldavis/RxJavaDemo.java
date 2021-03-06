package com.github.adamldavis;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.BiConsumer;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Demonstrates RxJava 2 in action.
 * 
 * @author Adam L. Davis
 */
public class RxJavaDemo implements ReactiveStreamsDemo {

   //questo è sincrono e sia il publisher(Flowable) che il subscriber (colui che consuma)
    //usano lo stesso thread
    public static List<Integer> doSquares() {
        List<Integer> squares = new ArrayList<>();
        Flowable.range(1, 64) //1
            .observeOn(Schedulers.computation()) //2
            .map(v -> v * v) //3
            .blockingSubscribe(squares::add); //4
        
        return squares;
    }

    //qui ho un thread principale associato al publisher (Flowable)
    // e poi per ogni elemento ho un Flowable che viene gestito con un thread
    //solo suo (vedi subscribeOn): ho quindi v sotto trhead dove v = numero di elementi (=64)
    public static List<Integer> doParallelSquares() {
        List<Integer> squares = new ArrayList<>();
        Flowable.range(1, 64)
            .flatMap(v -> //1  //la flatMap unisce tutti i risultati paralleli che senza flatMap sarebbero sincroni
              Flowable.just(v)
                      .doOnNext(j -> {System.out.println("sto elaborando item " + j + " - " + Thread.currentThread().getName()); })//viene eseguito il consumer fino a questo punto
                .subscribeOn(Schedulers.computation())//ad ogni flow viene dato un thread nuovo
                .map(w -> w * w) //se metto doOnNext dopo questo posso consumare e visualizzare ad esempio il quadrato w*w
            )
            .doOnError(ex -> ex.printStackTrace()) //2
            .doOnComplete(() -> System.out.println("Completed")) //3
            .blockingSubscribe(squares::add);
            
        return squares;
    }

    @Override
    public Future<List<Integer>> doSquaresAsync(int count) {
        return Flowable.range(1, count)
                .observeOn(Schedulers.computation()) 
                .map(v -> v * v)
                .collectInto((List<Integer>) new ArrayList<Integer>(), 
                        (integers, integer) -> integers.add(integer))
                .toFuture();
    }

    @Override
    public Future<String> doStringConcatAsync(int count) {
        return Observable.range(0, count).map(i -> "i=" + i)
                .collectInto(new StringBuilder(),
                    (stringBuilder, o) -> stringBuilder.append(o))
                .map(StringBuilder::toString)
                .toFuture();
    }

    @Override
    public Future<List<Integer>> doParallelSquaresAsync(int count) {
        return Flowable.range(1, count) //main thread
                .flatMap(v ->  //la flatMap unisce tutti i risultati paralleli che senza flatMap sarebbero sincroni
                    Flowable.just(v)
                            .subscribeOn(Schedulers.computation()) //ad ogni flow viene dato un thread nuovo
                            .map(w -> w * w)
                ).collectInto((List<Integer>) new ArrayList<Integer>(),
                        (integers, integer) -> integers.add(integer))
                .toFuture();
    }

    @Override
    public Future<String> doParallelStringConcatAsync(int count) {
        BiConsumer<StringBuilder, Object> collector =
                (stringBuilder, o) -> stringBuilder.append(o); //1
        return Observable.range(0, count).map(i -> "i=" + i)
                .window(10) // 3
                .flatMap(flow -> flow.subscribeOn(Schedulers.computation())
                        .collectInto(new StringBuilder(), collector).toObservable())
                .collectInto(new StringBuilder(), collector) //4
                .map(StringBuilder::toString) //5
                .toFuture();
    }

    public static void runComputation() {
        Flowable<String> source = Flowable.fromCallable(() -> { //1
            Thread.sleep(1000); //  imitate expensive computation
            return "Done";
        });
        source.doOnComplete(() -> System.out.println("Completed runComputation"));

        Flowable<String> background = source.subscribeOn(Schedulers.io()); //2

        Flowable<String> foreground = background.observeOn(Schedulers.single());//3

        foreground.subscribe(System.out::println, Throwable::printStackTrace);//4
    }
    
    public static void writeFile(File file) {
        try (PrintWriter pw = new PrintWriter(file)) {
            Flowable.range(1, 100)
                .observeOn(Schedulers.newThread())
                .blockingSubscribe(pw::println);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void readFile(File file) {
        try (final BufferedReader br = new BufferedReader(new FileReader(file))) {
            Flowable<String> flow = Flowable.fromPublisher(new FilePublisher(br));

            flow.observeOn(Schedulers.io())
                    .blockingSubscribe(System.out::println);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readFile2(File file) {
        Single<BufferedReader> readerSingle = Single.just(file) //1
                .observeOn(Schedulers.io()) //2
                .map(FileReader::new)
                .map(BufferedReader::new); //3
        Flowable<String> flowable = readerSingle.flatMapPublisher(reader -> //4
                Flowable.fromIterable( //5
                        () -> Stream.generate(readLineSupplier(reader)).iterator()
                ).takeWhile(line -> !"EOF".equals(line))); //6
        flowable
                .doOnNext(it -> System.out.println("thread="
                        + Thread.currentThread().getName())) //7
                .doOnError(ex -> ex.printStackTrace())
                .blockingSubscribe(System.out::println); //8
    }

    private static Supplier<String> readLineSupplier(BufferedReader reader) {
        return () -> { try {
                String line = reader.readLine();
                return line == null ? "EOF" : line;
            } catch (IOException ex) { throw new RuntimeException(ex); }};
    }

    public static int countUsingBackpressure(long sleepMillis) throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0); //1
        Flowable<Long> interval =
                Observable.interval(1, TimeUnit.MILLISECONDS) //2
                .toFlowable(BackpressureStrategy.LATEST) //3
                .take(2000); //4
        interval.subscribe(x -> {
            Thread.sleep(100); //5
            count.incrementAndGet();
        });
        Thread.sleep(sleepMillis); //6
        return count.get();
    }

    static class FilePublisher implements Publisher<String> {
        BufferedReader reader;
        public FilePublisher(BufferedReader reader) { this.reader = reader; }
        @Override
        public void subscribe(Subscriber<? super String> subscriber) {
            subscriber.onSubscribe(
	            	new FilePublisherSubscription(this, subscriber));
        }
        public String readLine() throws IOException {
            return reader.readLine();
        }
    }

    static class FilePublisherSubscription implements Subscription {
        FilePublisher publisher;
        Subscriber<? super String> subscriber;
        public FilePublisherSubscription( FilePublisher publisher, 
        		Subscriber<? super String> subscriber) {
            this.publisher = publisher;
            this.subscriber = subscriber;
        }
        @Override
        public void request(long n) {
            try {
                String line;
                for (int i = 0; i < n && publisher != null 
                			&& (line = publisher.readLine()) != null; i++) {
                    if (subscriber != null) subscriber.onNext(line);
                }
            } catch (IOException ex) {
                subscriber.onError(ex);
            }
            subscriber.onComplete();
        }
        @Override
        public void cancel() {
            publisher = null;
        }
    }

    /**
     * 8 *- RxComputationThreadPool-1
     * 1 **- RxNewThreadScheduler-1
     * 9 *- RxComputationThreadPool-1
     * 3 **- RxNewThreadScheduler-1
     * ...
     */
    public static void doParallelComputation2(){
        List<Integer> squares = new ArrayList<>();
        Flowable.range(1,64)
                .filter( v -> v % 2 == 0)
                .subscribeOn(Schedulers.computation()) //uso il computation thread per i numeri pari
                .map(w -> w/2)
                .doOnNext(w->System.out.println(w + " *- " + Thread.currentThread().getName()))
                .observeOn(Schedulers.newThread()) //cambio e passo ad un altro thread per il subscribe
                .filter(k -> k % 2 != 0) //e uso i numeri dispari
                .map(l -> l/1)
                .doOnNext(w->System.out.println(w + " **- " + Thread.currentThread().getName()))
                //qui sempre il main thread del publisher/Flowable che ha fatto partire il processo
                .blockingSubscribe(w-> System.out.println(w + " - " + Thread.currentThread().getName()));
    }

    /***es:
     *  item  4 on thread RxNewThreadScheduler-1
     *  item  6 on thread RxNewThreadScheduler-2
     *  item  7 on thread RxNewThreadScheduler-3
     * received item length 4 on thread main
     * received item length 6 on thread main
     * received item length 7 on thread main
     */
    public static void testParallelFlatMapMultiThread(){
        int i = 4;
        Flowable.just("long", "longer", "longest") //colui che emette ( observable o publisher(Flowable impl.  Publisher))
                .observeOn(Schedulers.computation()) //posso cambiare il thread che esegue la performLongOp se scommento
                .flatMap(
                        v -> performLongOp(v) //qua cè il thread main
                                .subscribeOn(Schedulers.newThread()) //usa un thread diverso per l'elaborazione dell 'op performLongOp
                                .doOnNext(k -> //questo è il subscribe (observer)
                                {   String time = Long.valueOf(System.nanoTime()).toString();
                                    System.out.println( " item  " + k + " on thread " + Thread.currentThread().getName()  + " - " + time);
                                    //if (k==4) { Thread.sleep(Integer.MAX_VALUE); } così si blocca solo il thread che elabora item 4, gli altri non si bloccano
                                })
                )
                .onErrorReturnItem(500)
                //.doOnNext( c-> {if (c==4) { Thread.sleep(Integer.MAX_VALUE); }})//si blocca il thread main del publisher/flow
                .blockingSubscribe( //questo è sempre il main thread di  del publisher/Flowable inziale (che emette)!
                        length ->System.out.println( "received item length " + length + " on thread " + Thread.currentThread().getName())
                        , ex -> System.out.println( "error on item: " + ex.getMessage() + ", thread " + Thread.currentThread().getName())
                );

    }

    /**
     *  item  4 on thread main
     *  item  6 on thread main
     *  item  7 on thread main
     * received item length 4 on thread main
     * received item length 6 on thread main
     * received item length 7 on thread main
     */
    public static void testParallelFlatMapSingleThread(){
        Flowable.just("long", "longer", "longest")
                .flatMap(
                        v -> performLongOp(v)
                                .doOnNext(k ->
                                        {   String time = Long.valueOf(System.nanoTime()).toString();
                                            System.out.println( " item  " + k + " on thread " + Thread.currentThread().getName()  + " - " + time);
                                        })
                                 //usa lo stesso thread di partenza in quanto manca il subscribeOn
                )
                .onErrorReturnItem(500)
                .blockingSubscribe(
                        length ->System.out.println( "received item length " + length + " on thread " + Thread.currentThread().getName())
                        , ex -> System.out.println( "error on item: " + ex.getMessage() + ", thread " + Thread.currentThread().getName())
                );

    }

    /**
     * Interrompe il flow appena incontra l'errore
     *  item  processed sucessfully 4 on thread RxNewThreadScheduler-1 ..
     * ..
     * The error message is: ERrrrrrrrrr!!
     * error on item: ERrrrrrrrrr!!, thread main
     */
    public static void testParallelFlatMapMultiThreadErrorHandler(){
        Flowable.just("long", "longer", "longest")
                .flatMap(
                        v -> performLongOpWIthError(v)//qua cè il thread main
                                .subscribeOn(Schedulers.newThread()) //da qua usa un thread diverso per il subscribe
                                .doOnNext(k -> System.out.println( " item  processed sucessfully " + k + " on thread " + Thread.currentThread().getName()))
                        //usa lo stesso thread di partenza in quanto manca il subscribeOn
                )
                //.onErrorReturnItem(-1)//al primo errore il flow si interrompe
                // e restituisce il codice -1, che veiene stampanto sotto come se tutto fosse andato a buon fine,
                // altrimenti scrive  l'ex sotto
                .doOnError(error -> System.err.println("The error message is: " + error.getMessage()))
                .blockingSubscribe(
                        length ->System.out.println( "received item length " + length + " on thread " + Thread.currentThread().getName())
                        , ex -> System.out.println( "error on item: " + ex.getMessage() + ", thread " + Thread.currentThread().getName())
                );

    }



    private static Flowable<Integer> performLongOp(String v){
        Random r = new Random ();
        try {
            int waiting = r.nextInt(10);
            String time = Long.valueOf(System.nanoTime()).toString();
            System.out.println(Thread.currentThread().getName() + " waiting " + (1000*waiting) +  " ms in time " + time);
                Thread.sleep(1000 * waiting);
            time = Long.valueOf(System.nanoTime()).toString();
            System.out.println(Thread.currentThread().getName() + " waiting finish in time  " + time);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Flowable.just(v.length());
    }

    private static Flowable<Object> performLongOpWIthError(String v)  throws Exception {
        Random r = new Random ();
        try {
            int waiting = r.nextInt(10);
            if (waiting % 2 == 0) {
            Thread.sleep(1000 * waiting);
            }
            else throw new Exception("ERrrrrrrrrr!!");

        } catch (Exception e) {
//            e.printStackTrace();
            throw e;
        }
        return Flowable.just(v.length());
    }


}
