package com.apache.minibase;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.Checksum;

public class DiskFile implements Closeable {

    public static final int BLOCK_SIZE_UP_LIMIT = 1024 * 1024 * 2;
    public static final int BLOOM_FILTER_HASH_COUNT = 3;
    public static final int BLOOM_FILTER_BITS_PER_KEY = 10;

    public static final int TRAILTER_SIZE = 8 + 4 + 8 + 8 + 8;
    public static final long DISK_FILE_MAGIC = 0xFAC881234221FFA9L;

    private String fname;
    private RandomAccessFile in;
    private SortedSet<BlockMeta> blockMetaSet = new TreeSet<>();

    private long fileSize;
    private int blockCount;
    private int blockIndexOffset;
    private long blockIndexSize;

    @Override
    public void close() throws IOException {

    }
}

class BlockMeta implements Comparable<BlockMeta> {

    private static final int OFFSET_SIZE = 8;
    private static final int SIZE_SIZE = 8;
    private static final int BF_LEN_SIZE = 4;

    private KeyValue lastKV;
    private long blockOffset;
    private long blockSize;
    private byte[] bloomFilter;

    public BlockMeta(KeyValue lastKV, long offset, long size, byte[] bloomFilter) {
        this.lastKV = lastKV;
        this.blockOffset = offset;
        this.blockSize = size;
        this.bloomFilter = bloomFilter;
    }

    public KeyValue getLastKV() {
        return this.lastKV;
    }

    public long getBlockOffset() {
        return this.blockOffset;
    }

    public long getBlockSize() {
        return this.blockSize;
    }

    public byte[] getBloomFilter() {
        return this.bloomFilter;
    }

    public int getSerializeSize() {
        return lastKV.getSerializeSize() + OFFSET_SIZE + SIZE_SIZE + BF_LEN_SIZE + bloomFilter.length;
    }


    private static BlockMeta createSeekDummy(KeyValue lastKv) {
        return new BlockMeta(lastKv, 0L, 0L, Bytes.EMPTY_BYTES);
    }

    // Encode
    public byte[] toBytes() throws IOException {
        byte[] bytes = new byte[getSerializeSize()];
        int pos = 0;

        byte[] kvBytes = lastKV.toBytes();
        System.arraycopy(kvBytes, 0, bytes, pos, kvBytes.length);
        pos += kvBytes.length;

        byte[] offsetBytes = Bytes.toBytes(blockOffset);
        System.arraycopy(offsetBytes, 0, bytes, pos, offsetBytes.length);
        pos += offsetBytes.length;

        byte[] bfLenBytes = Bytes.toBytes(bloomFilter.length);
        System.arraycopy(bfLenBytes, 0, bytes, pos, bfLenBytes.length);
        pos += bfLenBytes.length;

        System.arraycopy(bloomFilter, 0, bytes, pos, bloomFilter.length);
        pos += bloomFilter.length;

        if (pos != bytes.length)
            throw new IOException(
                    "pos(" + pos + ") should be equal to length of bytes (" + bytes.length + ")"
            );
        return bytes;
    }

    // Decode
    public static BlockMeta parseFrom(byte[] buf, int offset) throws IOException {
        int pos = offset;

        // Decode last key value
        KeyValue lastKv = KeyValue.parseForm(buf, offset);
        pos += lastKv.getSerializeSize();

        // Decode block blockOffset
        long blockOffset = Bytes.toLong(Bytes.slice(buf, pos, OFFSET_SIZE));
        pos += OFFSET_SIZE;

        // Decode block blockSize
        long blockSize = Bytes.toLong(Bytes.slice(buf, pos, SIZE_SIZE));
        pos += SIZE_SIZE;

        // Decode blockSize of block bloom filter
        int bloomFilterSize = Bytes.toInt(Bytes.slice(buf, pos, BF_LEN_SIZE));
        pos += BF_LEN_SIZE;

        // Decode bytes of block bloom filter
        byte[] bloomFilter = Bytes.slice(buf, pos, bloomFilterSize);
        pos += bloomFilterSize;

        assert pos <= buf.length;
        return new BlockMeta(lastKv, blockOffset, blockSize, bloomFilter);
    }

    @Override
    public int compareTo(BlockMeta o) {
        return this.lastKV.compareTo(o.lastKV);
    }
}

class BlockIndexWriter {
    private List<BlockMeta> blockMetas = new ArrayList<>();
    private int totalBytes = 0;

    public void append(KeyValue lastKV, long offset, long size, byte[] bloomFilter) {
        BlockMeta meta = new BlockMeta(lastKV, offset, size, bloomFilter);
        blockMetas.add(meta);
        totalBytes += meta.getSerializeSize();
    }

    public byte[] serialize() throws IOException {
        byte[] buffer = new byte[totalBytes];
        int pos = 0;
        for (BlockMeta meta : blockMetas) {
            byte[] metaBytes = meta.toBytes();
            System.arraycopy(metaBytes, 0, buffer, pos, metaBytes.length);
            pos += metaBytes.length;
        }
        assert pos == totalBytes;
        return buffer;
    }
}

class BlockWriter {
    public static final int KV_SIZE_LEN = 4;
    public static final int CHECKSUM_LEN = 4;

    private int totalSize;
    private List<KeyValue> kvBuf;
    private BloomFilter bloomFilter;
    private Checksum crc32;
    private KeyValue lastKV;
    private int KeyValueCount;
}
