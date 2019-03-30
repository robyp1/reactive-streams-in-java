package com.github.adamldavis;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.context.Context;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.github.adamldavis.DemoData.squares;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ReactorDemoTest {


    ReactorDemo demo = new ReactorDemo();

    @Test
    public void testDoSquares() {
        assertArrayEquals(squares.toArray(), demo.doSquares().toArray());
    }

    @Test
    public void testDoParallelSquares() {
        List result = demo.doParallelSquares()
                .stream().sorted().collect(Collectors.toList());
                
        assertArrayEquals(squares.toArray(), result.toArray());
    }

    @Test
    public void testStepVerifier_Mono_error() {
        Mono<String> monoError = Mono.error(new RuntimeException("error"));

        StepVerifier.create(monoError)
                .expectErrorMessage("error")
                .verify();
    }
    @Test
    public void testStepVerifier_Mono_empty() {
        Mono<String> monoError = Mono.empty();

        StepVerifier.create(monoError)
                .expectComplete()
                .verify();
    }

    @Test
    public void testStepVerifier_Mono_foo() {
        Mono<String> foo = Mono.just("foo");
        StepVerifier.create(foo)
                .expectNext("foo")
                .verifyComplete();
    }

    @Test
    public void testStepVerifier_Flux() {
        Flux<Integer> flux = Flux.just(1, 4, 9);

        StepVerifier.create(flux)
                .expectNext(1)
                .expectNext(4)
                .expectNext(9)
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    public void testStepVerifier_Context_Wrong() {
        Flux<Integer> flux = Flux.just(1).subscriberContext(Context.of("pid", 123));

        Flux<String> stringFlux = flux.flatMap(i ->
                        Mono.subscriberContext().map(ctx -> i + " pid: " + ctx.getOrDefault("pid", 0)));

        StepVerifier.create(stringFlux)
                .expectNext("1 pid: 0")
                .verifyComplete();
    }



    @Test
    public void testStepVerifier_Context_Right() {
        Flux<Integer> flux = Flux.just(1);

        Flux<String> stringFlux = flux.flatMap(i ->
                Mono.subscriberContext().map(ctx -> i + " pid: " + ctx.getOrDefault("pid", 0)));

        StepVerifier.create(stringFlux.subscriberContext(Context.of("pid", 123)))
                .expectNext("1 pid: 123")
                .verifyComplete();
    }

    @Test
    public void testStepVerifier_Context_Right2() {
        Flux<Integer> flux = Flux.just(1,2,3);
        Flux<String> stringFlux = flux.flatMap(i -> Mono.subscriberContext().map(ctx -> i + " pid: " + ctx.getOrDefault("pid", 0)));

        StepVerifier.create(stringFlux.subscriberContext(Context.of("pid", 123)))
                .expectNext("1 pid: 123")
                .expectNext("2 pid: 123")
                .expectNext("3 pid: 123")
                .verifyComplete();
    }

    @Test
    public void test_TestPublisher() {
        TestPublisher<Object> publisher = TestPublisher.create(); //1
        Flux<Object> stringFlux = publisher.flux(); //2
        List list = new ArrayList(); //3

        stringFlux.subscribe(next -> list.add(next), ex -> ex.printStackTrace()); //4
        publisher.emit("foo", "bar"); //5

        assertEquals(2, list.size()); //6
        assertEquals("foo", list.get(0));
        assertEquals("bar", list.get(1));
    }

    @Test
    public void create_vs_range() {
        Flux<String> flux1 = Flux.create(sink -> {
            for (int i = 0; i < 3; i++) {
                sink.next("i=" + i);
            }
            sink.complete();
        });
        // is identical to:
        Flux<String> flux2 = Flux.range(0, 3)
                .map(i -> "i=" + i);

        StepVerifier.create(flux1)
                .expectNext("i=0")
                .expectNext("i=1")
                .expectNext("i=2")
                .verifyComplete();
        StepVerifier.create(flux2)
                .expectNext("i=0")
                .expectNext("i=1")
                .expectNext("i=2")
                .verifyComplete();
    }

    @Test(timeout = 1000)
    public void testpush() {
        List<Integer> list = Flux.push((FluxSink<Integer> sink) -> {
            sink.next(1).next(2).next(3).complete();
        }).collectList().block();

        assertEquals(3, list.size());
    }

    @Test(timeout = 1000)
    public void testGenerate() {
        Flux<Long> flux = ReactorDemo.exampleSquaresUsingGenerate();
        List<Long> list = flux.collectList().doOnNext(System.out::println).block();

        assertEquals(11, list.size());
    }


    @Test
    public void testdoParallelStringConcatAsync() throws ExecutionException, InterruptedException {
        System.out.println(demo.doParallelStringConcatAsync(20).get());
    }

    /**
     * estrae da un file txt di un libro l'autore e il titolo, rimpiazza autore con by e toglie title:
     * concatena poi le due stringhe estratte titolo + by Autore e le stampa con il donext sulla console
     * @throws URISyntaxException
     */
    @Test
    public void testReadFile() throws URISyntaxException {
        URI uri = ClassLoader.getSystemResource("prova.txt").toURI();//vedere in test/resources
        FileStreamReader fileStreamReader = new FileStreamReader();
        fileStreamReader.fluxVersion(Paths.get(uri))
        .doOnNext(System.out::println).blockLast();

    }
}
