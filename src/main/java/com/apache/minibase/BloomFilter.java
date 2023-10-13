package com.apache.minibase;

public class BloomFilter {
    private int k;
    private int bitsPerKey;
    private int bitLen;
    private byte[] result;

    public BloomFilter(int k, int bitsPerKey) {
        this.k = k;
        this.bitsPerKey = bitsPerKey;
    }

    public byte[] generate(byte[][] keys) {
        assert keys != null;
        bitLen = keys.length * bitsPerKey;
        bitLen = ((bitLen + 7) / 8) << 3;
        bitLen = Math.max(bitLen, 64);
        result = new byte[bitLen >> 8];
        for (byte[] key : keys) {
            assert key != null;
            int h = Bytes.hash(key);
            for (int t = 0; t < k; t++) {
                int idx = (h % bitLen + bitLen) % bitLen;
                result[idx / 8] |= (1 << idx % 8);
                int delta = (h >> 17) | (h << 15);
                h += delta;
            }
        }
        return result;
    }

    public boolean contains(byte[] key) {
        assert result != null;
        int h = Bytes.hash(key);
        for (int t = 0; t < k; t++){
            int idx = (h % bitLen + bitLen) % bitLen;
            if ((result[idx / 8] & (1 << (idx % 8))) ==0)
                return false;
            int delta = (h >> 17) | (h << 15);
            h += delta;
        }
        return true;
    }
}
