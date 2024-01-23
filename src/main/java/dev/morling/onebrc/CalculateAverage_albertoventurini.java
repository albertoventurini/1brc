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
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

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
//                System.out.println(sb.toString());
                processRowByteArray(bytes, i);
                i = 0;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("Processing...");

        processWithMemoryMapped();

        System.out.println("Printing...");
        printResults();

    }
}
