package com.github.adamldavis;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.BaseStream;

public class FileStreamReader {

    private Flux<String> path;

    /**
     * crea il flusso che emette una riga alla volta nel file
     * @param path
     * @return
     */
    public Flux<String>  fromPath (final Path path){
       return  Flux.using(
                () ->  Files.lines(path),
                Flux::fromStream,
                BaseStream::close);

    }

    public Flux<String> fluxVersion(Path path){
        final Runtime runtime = Runtime.getRuntime();

        return fromPath(path)
                .filter(s -> s.startsWith("Title: ") || s.startsWith("Author: ")
                        || s.equalsIgnoreCase("##BOOKSHELF##"))
                .map(s -> s.replaceFirst("Title: ", ""))
                .map(s -> s.replaceFirst("Author: ", " by "))
                .windowWhile(s -> !s.contains("##"))//per ogni elemento del flux crea un flux che lo contiene e lo fa per ognuno finche la condiz nn diventa false
                .flatMap(bookshelf -> bookshelf
                        .window(2) // per ogni sequenda di Flux prende due flux (titolo e autore)
                        .subscribeOn(Schedulers.parallel())
                        .flatMap(bookinfo -> bookinfo.reduce(String::concat))
                        .collectList()
                        .doOnNext(s->System.gc())
                        .flatMapMany( bookList ->Flux.just(
                                "\n\nFound new Bookshelf of " + bookList.size() + " books:",
                                bookList.toString(),
                                String.format("Memory in use while reading: %dMB\n", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024))
                        )));


    }

    public void extractLinesParallel(Path path){
          fromPath(path)
                .windowWhile(s -> !s.contains("##"))
                .window(10)
                .flatMap(v-> v.flatMap(k -> k)
                  .subscribeOn(Schedulers.newParallel("parall-")))
                  .doOnNext(System.out::println)
                  .log()
                  .blockLast();

    }
}
