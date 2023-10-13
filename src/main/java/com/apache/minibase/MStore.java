package com.apache.minibase;

import java.io.IOException;

public class MStore implements MiniBase{

    @Override
    public void put(byte[] key, byte value) throws Exception {

    }

    @Override
    public KeyValue get(byte[] key) throws Exception {
        return null;
    }

    @Override
    public void delete(byte[] key) throws IOException {

    }

    @Override
    public Iter<KeyValue> scan(byte[] startKey, byte[] stopKey) throws IOException {
        return null;
    }
}
