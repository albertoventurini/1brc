/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import javax.swing.tree.TreeNode;
import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

import static java.lang.Math.round;
import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_albertoventurini {

    private static class TrieNode {
        TrieNode[] children = new TrieNode[256];
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static final TrieNode root = new TrieNode();

    private static void processRow(final String row) {
        final String[] tokens = row.split(";");
        byte[] bytes = tokens[0].getBytes();

        TrieNode node = root;
        for (int i = 0; i < bytes.length; i++) {
            final int b = bytes[i] & 0xFF;
            if (node.children[b] == null) {
                node.children[b] = new TrieNode();
            }
            node = node.children[b];
        }

        double value = Double.parseDouble(tokens[1]);

        node.min = Math.min(node.min, value);
        node.max = Math.max(node.max, value);
        node.sum += value;
        node.count++;
    }

    private static void processRow2(final String row) {
        byte[] bytes = row.getBytes();

        TrieNode node = root;
        int i = 0;
        while (bytes[i] != ';') {
            final int b = bytes[i] & 0xFF;
            if (node.children[b] == null) {
                node.children[b] = new TrieNode();
            }
            node = node.children[b];
            i++;
        }

        i++;

        double reading = 0.0;
        boolean parsingDecimal = false;
        while (i < row.length()) {
            if (bytes[i] == '.') {
                parsingDecimal = true;
                i++;
                continue;
            }

            if (parsingDecimal) {
                // todo
            }
            else {
                reading = reading * 10 + (bytes[i] - '0');
            }

            i++;
        }

        node.min = Math.min(node.min, reading);
        node.max = Math.max(node.max, reading);
        node.sum += reading;
        node.count++;
    }

    private static void processRow(final ChunkReader cr) {
        TrieNode node = root;

        while (cr.peekNext() != ';') {
            final int b = cr.getNext() & 0xFF;
            if (node.children[b] == null) {
                node.children[b] = new TrieNode();
            }
            node = node.children[b];
        }

        cr.getNext();
        double reading = 0.0;
        boolean parsingDecimal = false;
        while (cr.peekNext() != '\n') {
            char c = cr.getNext();
            if (c == '.') {
                parsingDecimal = true;
                cr.getNext();
                continue;
            }

            if (parsingDecimal) {
                // todo
            }
            else {
                reading = reading * 10 + (c - '0');
            }
        }

        cr.getNext();

        node.min = Math.min(node.min, reading);
        node.max = Math.max(node.max, reading);
        node.sum += reading;
        node.count++;
    }

    private static void printResults() {
        byte[] bytes = new byte[100];

        printResultsRec(root, bytes, 0);
    }

    private static void printResultsRec(final TrieNode node, final byte[] bytes, final int index) {
        if (node.count > 0) {
            final String location = new String(bytes, 0, index);
            System.out.println(location + "=" + round(node.min) + "/" + round(0) + "/" + round(node.max));
        }

        for (int i = 0; i < 256; i++) {
            if (node.children[i] != null) {
                bytes[index] = (byte) i;
                printResultsRec(node.children[i], bytes, index + 1);
            }
        }
    }

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static class ChunkReader {
        char[] chunk = new char[10_000_000];

        Reader reader;

        int readChars;

        int cursor = 0;

        public ChunkReader(Reader reader) {
            this.reader = reader;
            readNextChunk();
        }

        boolean hasNext() {
            return readChars != -1;
        }

        char getNext() {
            if (cursor >= readChars) {
                readNextChunk();
            }
            return chunk[cursor++];
        }

        char peekNext() {
            if (cursor >= readChars) {
                readNextChunk();
            }
            return chunk[cursor];
        }

        private void readNextChunk() {
            try {
                readChars = reader.read(chunk);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            cursor = 0;
        }
    }

    public static void main(String[] args) throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        // RandomAccessFile memoryMappedFile = new RandomAccessFile(FILE, "r");
        // try (FileChannel channel = FileChannel.open(Path.of(FILE))) {
        // long length = channel.size();
        // MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, length);
        //
        // for (int i = 0; i < length; i++) {
        // buffer.get();
        // }
        //
        // }

        System.out.println("Processing...");
        BufferedReader reader;

        try {
            reader = new BufferedReader(new FileReader(FILE), 8192 * 10);
            String line = reader.readLine();

            while (line != null) {
                processRow2(line);
                line = reader.readLine();
            }

            // char[] dest = new char[10_000_000];
            //
            // int c = reader.read(dest);
            // while (c != -1) {
            // c = reader.read(dest);
            // }

            // ChunkReader cr = new ChunkReader(reader);
            // while (cr.hasNext()) {
            // processRow(cr);
            // }

            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Printing...");
        printResults();

        // Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
        // MeasurementAggregator::new,
        // (a, m) -> {
        // a.min = Math.min(a.min, m.value);
        // a.max = Math.max(a.max, m.value);
        // a.sum += m.value;
        // a.count++;
        // },
        // (agg1, agg2) -> {
        // var res = new MeasurementAggregator();
        // res.min = Math.min(agg1.min, agg2.min);
        // res.max = Math.max(agg1.max, agg2.max);
        // res.sum = agg1.sum + agg2.sum;
        // res.count = agg1.count + agg2.count;
        //
        // return res;
        // },
        // agg -> {
        // return new ResultRow(agg.min, (round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
        // });
        //
        // Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
        // .map(l -> new Measurement(l.split(";")))
        // .collect(groupingBy(m -> m.station(), collector)));
        //
        // System.out.println(measurements);
    }
}
