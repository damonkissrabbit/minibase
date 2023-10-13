package com.apache.demo;

public class toByte {
    public static void main(String[] args) {
        int intValue = 12345;
        byte[] byteArray = new byte[4];

        for (int i = 0; i < 4; i++){
            int offset = (3 - i) << 3;
            byteArray[i] = (byte) ((intValue >> offset) & 0xFF);
        }

        for (byte b : byteArray) {
            System.out.println(b);
        }
    }
}
