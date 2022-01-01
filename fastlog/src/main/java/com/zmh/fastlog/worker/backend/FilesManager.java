package com.zmh.fastlog.worker.backend;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.val;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;
import static lombok.AccessLevel.PRIVATE;

@FieldDefaults(level = PRIVATE, makeFinal = true)
final class FilesManager implements AutoCloseable {

    AtomicInteger index;

    Queue<Path> queue;

    Path folder;

    String prefix;

    String suffix;

    Pattern fileNamePattern;

    Pattern fileIndexPattern;

    @Builder
    FilesManager(@NonNull Path folder, String prefix, String suffix) {
        index = new AtomicInteger(0);

        this.folder = folder;
        this.prefix = ofNullable(prefix)
            .map(String::trim)
            .orElse("");
        this.suffix = ofNullable(suffix)
            .map(String::trim)
            .orElse("");

        val fileNameRegex = String.format(ENGLISH, "^%s\\d+%s$", this.prefix, this.suffix);
        fileNamePattern = Pattern.compile(fileNameRegex);

        val fileIndexRegex = String.format(ENGLISH, "^%s(?<index>\\d+)%s$", this.prefix, this.suffix);
        fileIndexPattern = Pattern.compile(fileIndexRegex);

        queue = getFilesFromFileSystem();
        if (!queue.isEmpty()) {
            val array = queue.toArray(new Path[0]);
            Path lastPath = array[array.length - 1];
            val lastPathIndex = getIndex(lastPath);
            index.set(lastPathIndex + 1);
        }
    }

    @Override
    public void close() {
        queue.clear();
    }

    Queue<Path> getFilesFromQueue() {
        return queue;
    }

    @SneakyThrows
    Queue<Path> getFilesFromFileSystem() {
        val file = folder.toFile();
        val list = file.list();
        if (list != null && list.length == 0) {
            return new LinkedList<>();
        }

        val byNamePattern = (Predicate<Path>) path ->
            ofNullable(path)
                .map(Path::getFileName)
                .map(Path::toString)
                .map(fileNamePattern::matcher)
                .map(Matcher::matches)
                .orElse(false);

        return Files.list(folder)
            .filter(Files::isRegularFile)
            .filter(byNamePattern)
            .sorted(comparing(this::getIndex))
            .collect(toCollection(LinkedList::new));
    }

    int getIndex(@NonNull Path path) {
        return of(path)
            .map(Path::getFileName)
            .map(Path::toString)
            .map(fileIndexPattern::matcher)
            .filter(Matcher::find)
            .map(matcher -> matcher.group("index"))
            .map(Integer::valueOf)
            .orElseThrow(() -> {
                val msg = String.format(ENGLISH, "File '%s' doesn't have index group", path.toString());
                return new IllegalArgumentException(msg);
            });
    }

    Path getFile(int fileIndex) {
        val fileName = String.format(ENGLISH, "%s%d%s", prefix, fileIndex, suffix);
        return folder.resolve(fileName);
    }

    @SneakyThrows
    Path createNextFile() {
        Path result;
        do {
            val nextIndex = index.getAndIncrement();
            result = getFile(nextIndex);
        } while (Files.exists(result));

        Files.createFile(result);
        queue.add(result);
        return result;
    }

    Path poll() {
        return queue.poll();
    }

    @SneakyThrows
    void remove(@NonNull Path... paths) {
        for (val path : paths) {
            Files.deleteIfExists(path);
            queue.remove(path);
        }
    }

    void remove(@NonNull Collection<Path> paths) {
        val array = paths.toArray(new Path[0]);
        remove(array);
    }

    void clear() {
        remove(queue);
        index.set(0);
    }
}
