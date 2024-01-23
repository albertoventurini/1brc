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

import java.io.IOException;
import java.io.RandomAccessFile;

public class CalculateAverage_albertoventurini {

    private static class TrieNode {
        final TrieNode[] children = new TrieNode[256];
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum;
        private long count;
    }

    private static final TrieNode root = new TrieNode();


    private static void processRow(final ChunkReader cr) {
        TrieNode node = root;

        int b = cr.getNext() & 0xFF;
        while (b != ';') {
            if (node.children[b] == null) {
                node.children[b] = new TrieNode();
            }
            node = node.children[b];
            b = cr.getNext() & 0xFF;
        }

        cr.advance();

        long reading = 0;
        boolean negative = false;
        while (true) {
            byte c = cr.getNext();
            if (c == '\n') {
                break;
            }
            if (c == '-') {
                negative = true;
                continue;
            }
            if (c == '.') {
                continue;
            }
            reading = reading * 10 + (c - '0');
        }

        final long signedReading = negative ? -reading : reading;

        node.min = Math.min(node.min, signedReading);
        node.max = Math.max(node.max, signedReading);
        node.sum += signedReading;
        node.count++;
    }

    private static void printResults() {
        byte[] bytes = new byte[100];

        printResultsRec(root, bytes, 0);
    }

    private static double round(long value) {
        return value / 10.0;
    }

    private static void printResultsRec(final TrieNode node, final byte[] bytes, final int index) {
        if (node.count > 0) {
            final String location = new String(bytes, 0, index);
            System.out.println(location + "=" + round(node.min) + "/" + node.sum / node.count + "/" + round(node.max));
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

        void advance() {
            cursor++;
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


    public static void main(String[] args) throws Exception {

        System.out.println("Processing...");

        processWithChunkReader();

        System.out.println("Printing...");
        printResults();

    }
}
