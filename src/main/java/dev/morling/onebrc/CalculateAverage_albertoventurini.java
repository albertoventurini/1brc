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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.*;

import static java.lang.Math.round;

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
        int decimalDigits = 0;
        while (i < row.length()) {
            if (bytes[i] == '.') {
                parsingDecimal = true;
                i++;
                continue;
            }

            if (parsingDecimal) {
                decimalDigits++;
            }

            reading = reading * 10 + (bytes[i] - '0');

            i++;
        }

        reading = reading / (double) decimalDigits;

        node.min = Math.min(node.min, reading);
        node.max = Math.max(node.max, reading);
        node.sum += reading;
        node.count++;
    }

    private static void processRowByteArray(final byte[] bytes, final int length) {

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
        int decimalDigits = 0;
        while (i < length) {
            if (bytes[i] == '.') {
                parsingDecimal = true;
                i++;
                continue;
            }

            if (parsingDecimal) {
                decimalDigits++;
            }

            reading = reading * 10 + (bytes[i] - '0');

            i++;
        }

        reading = reading / (double) decimalDigits;

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
        int decimalDigits = 0;
        while (cr.hasNext() && cr.peekNext() != '\n') {
            byte c = cr.getNext();
            if (c == '.') {
                parsingDecimal = true;
                cr.getNext();
                continue;
            }

            if (parsingDecimal) {
                decimalDigits++;
            }
            else {
                reading = reading * 10 + (c - '0');
            }
        }

        reading = reading / (double) decimalDigits;

        node.min = Math.min(node.min, reading);
        node.max = Math.max(node.max, reading);
        node.sum += reading;
        node.count++;

        if (cr.hasNext()) {
            cr.getNext();
        }
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

    private static class ChunkReader {
        int len = 10_000_000;
        byte[] chunk = new byte[len];

        RandomAccessFile file;

        int readChars;

        int cursor = 0;
        long off = 0;


        long fileLength;


        public ChunkReader(RandomAccessFile file) throws IOException {
            this.file = file;
            this.fileLength = file.length();
            readNextChunk();
        }

        boolean hasNext() {
            return (off + cursor) < fileLength;
        }

        byte getNext() {
            if (cursor >= readChars) {
                readNextChunk();
            }
            return chunk[cursor++];
        }

        byte peekNext() {
            if (cursor >= readChars) {
                readNextChunk();
            }
            return chunk[cursor];
        }

        private void readNextChunk() {
            try {
                file.seek(off);
                readChars = file.read(chunk);
                off += readChars;
                cursor = 0;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void processWithMultiThread() {
        int nThreads = 256;
        BufferedReader reader;

        BlockingQueue[] queues = new LinkedBlockingQueue[nThreads];

        final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

        for (int i = 0; i < nThreads; i++) {
            final int index = i;
            queues[i] = new LinkedBlockingQueue<String>();

            executorService.submit(() -> {
                while (true) {
                    final String line;
                    try {
                        line = (String) queues[index].take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    if (line.equals("===")) {
                        break;
                    } else {
                        processRow2(line);
                    }
                }
            });
        }

        int j = 0;

        try {
            reader = new BufferedReader(new FileReader(FILE));
            String line = reader.readLine();

            while (line != null) {
                int queueIndex = line.charAt(0) & 0xFF;

                if ((j % 1_000_000) == 0) {
                    System.out.println(j);
                }
                j++;

                queues[queueIndex].put(line);
                line = reader.readLine();
            }

            reader.close();

            for (int i = 0; i < nThreads; i++)
                queues[i].put("===");

        }  catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Finished");

        executorService.shutdown();

        System.out.println("Shut down");
    }

    private static void processWithBufferedReader() {
        BufferedReader reader;

        try {
            reader = new BufferedReader(new FileReader(FILE));
            String line = reader.readLine();

            while (line != null) {
                processRow2(line);
                line = reader.readLine();
            }

            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processWithChunkReader() throws Exception {
        var randomAccessFile = new RandomAccessFile(FILE, "r");

        try {
            var chunkReader = new ChunkReader(randomAccessFile);

            while (chunkReader.hasNext()) {
                processRow(chunkReader);
            }
            randomAccessFile.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processWithMemoryMapped() throws Exception {
        FileChannel channel = FileChannel.open(Path.of(FILE));

        long chunkLength = Integer.MAX_VALUE;
        long fileSize = channel.size();
        long position = 0;

        long index = 0;

        byte[] bytes = new byte[200];
        int i = 0;

        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, chunkLength);

        while (true) {
            if (index >= chunkLength) {

                if ((position + chunkLength) > fileSize) {
                    break;
                }

                buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, chunkLength);
                position += chunkLength;
                index = 0;
            }

            byte b = buffer.get();
            index++;

            if (b != '\n') {
                bytes[i++] = b;
            } else {
                processRowByteArray(bytes, i);
                i = 0;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("Processing...");

        processWithChunkReader();

        System.out.println("Printing...");
        printResults();

    }
}
