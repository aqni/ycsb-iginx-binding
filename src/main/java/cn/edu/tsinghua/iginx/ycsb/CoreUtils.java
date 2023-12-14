package cn.edu.tsinghua.iginx.ycsb;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;

import java.util.Base64;

public class CoreUtils {
    public static long getTimestamp(String key) {
        return Long.parseLong(key.substring(4));
    }

    public static String getFieldName(String path) {
        return path.substring(path.indexOf('.') + 1);
    }

    public static byte[] getValue(ByteIterator iterator) {
        return Base64.getEncoder().encode(iterator.toArray());
    }

    public static ByteIterator getByteIterator(byte[] value) {
        return new ByteArrayByteIterator(Base64.getDecoder().decode(value));
    }
}
