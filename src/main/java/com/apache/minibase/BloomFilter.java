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
        bitLen = ((bitLen + 7) / 8) << 3;  // 将比特位数转换为字节数，加7是为了向上取整到最近8的倍数，然后右移三位（相当于除以8）得到字节数
        bitLen = Math.max(bitLen, 64);  // 确保最后得到的字节数至少为64，保证布隆过滤器的最小大小
        result = new byte[bitLen >> 8];  // 创建一个字节数组，存储布隆过滤器的比特位信息
        for (byte[] key : keys) {   // 遍历二维数组的每一行
            assert key != null;
            int h = Bytes.hash(key);  // 计算当前行的hash值
            for (int t = 0; t < k; t++) {
                // h 除以 bitLen 确保了结果在0~bitLen-1 之间，因为取余操作得结果总是非负数
                // + bitLen为了保证结果值非负，将bitLen加到计算结果上，这样，无论取余操作的结果是正数还是负数，都可以确保结果大于等于bitLen
                // 最后进行取余操作，这个操作是为了确保idx在0到bitLen之间，因为加上bitLen之后再次取余，可以使得结果限制在合法的范围内
                int idx = (h % bitLen + bitLen) % bitLen;  // 计算当前哈希值对应的比特位在result数组中的索引位置，确保索引值为非负数
                // 1 << idx % 8 使用左移运算符将1左移指定的位数，在这里将1左移 idx % 8位，得到一个只有第 idx % 8 位是1，其余位都是0的二进制位
                // idx / 8, 计算了idx对应的字节数组（result数组）的索引，由于每个字节战8位，idx/8就是idx处在字节数组中的索引位置
                // result[idx / 8] |= (1 << idx % 8) 是一个按位或运算操作，它的目的是将字节数组result中的第idx位设置为1，具体做法是，将 1 << idx % 8 得到的二进制数
                // 与 result[idx / 8]进行按位或操作，将idx处的比特位设置为1
                // 通过这个操作，可以将布隆过滤器中对应索引idx处的比特位设置为1，表示该位置代表的元素在布隆过滤器中存在，这种技巧是布隆过滤器的基本原理，通过多次哈希和位运算，将输入的元素映射到布隆过滤器的比特微数组中
                result[idx / 8] |= (1 << idx % 8);   // 将对应的比特位记为1，使用位运算操作实现
                int delta = (h >> 17) | (h << 15); // 计算下一个哈希值的增量，用于产生不同的哈希函数
                h += delta;   // 更新哈希值，准备计算你下一个哈希函数的位置
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
