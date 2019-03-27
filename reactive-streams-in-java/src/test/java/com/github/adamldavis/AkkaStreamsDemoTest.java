package com.github.adamldavis;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.github.adamldavis.DemoData.squares;
import static org.junit.Assert.*;

public class AkkaStreamsDemoTest {

    AkkaStreamsDemo demo = new AkkaStreamsDemo();

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
        
    Channel channel = new Channel();

    @Test
    public void testPrintErrors() {
        // given
        demo.setChannel(channel); //consuma i messaggi
        // when
        demo.printErrors(); //scrivi dagli 8 ai 16 messaggi nella akka source

        int count = 201;

        channel.publish("OK ");//questo viene filtrato e scartato dal printErrors

        for (int i = 0; i < count; i++) {//scrive sulla deque che verrà processata dalla poll (Vedi in set chanel), la source viene riempita (max 16)
            //o fino ad un minimo di 8 msg, dopodichè la publish scrive sulla queue e la poll processa un elemento alla volta man mano che questo è resto disponilbile
            channel.publish("Error: " + i);
            System.out.println("Publish " + System.nanoTime());
        }
        try { Thread.sleep(2000); } catch (Exception e) { throw new RuntimeException(e); }
        // then
        assertFalse(demo.messageList.isEmpty());
        assertEquals(count, demo.messageList.size());
        assertNotNull(demo.publisher);
    }


    ActorSystem system;
    ActorMaterializer materializer;
    @Before
    public void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }
    @After
    public void tearDown() {
        akka.testkit.javadsl.TestKit.shutdownActorSystem(system);
    }

    @Test
    public void test_a_source() {
        Sink<Object, TestSubscriber.Probe<Object>> sink = TestSink.probe(system);
        Source<Object, NotUsed> sourceUnderTest = Source.single("test");

        sourceUnderTest.runWith(sink, materializer)
                .request(1)
                .expectNext("test")
                .expectComplete();
    }

    @Test
    public void test_a_sink() throws TimeoutException, InterruptedException {
        Sink<String, CompletionStage<List<String>>> sinkUnderTest = Sink.seq();
        final Pair<TestPublisher.Probe<String>, CompletionStage<List<String>>> stagePair =
                TestSource.<String>probe(system)
                        .toMat(sinkUnderTest, Keep.both())
                        .run(materializer);
        final TestPublisher.Probe<String> probe = stagePair.first();
        final CompletionStage<List<String>> future = stagePair.second();
        probe.expectRequest();
        probe.sendNext("test");
        probe.sendError(new Exception("boom!"));
        try {
            future.toCompletableFuture().get(2, TimeUnit.SECONDS);
            assert false;
        } catch (ExecutionException ee) {
            final Throwable exception = ee.getCause();
            assertEquals(exception.getMessage(), "boom!");
        }
    }

    @Test
    public void testGraph() {
        demo.saveTextFileUsingGraph(Arrays.asList("foo", "bar"));
    }

}
