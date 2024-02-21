package cn.edu.tsinghua.iginx.ycsb;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;

public class CoreUtils {
  public static long getTimestamp(String key) {
    return Long.parseLong(key.substring(4));
  }

  public static String getFieldName(String path) {
    return path.substring(path.indexOf('.') + 1);
  }

  public static String getPath(String table, String field) {
    return table + "." + field;
  }

  public static byte[] getValue(ByteIterator iterator) {
    return iterator.toArray();
  }

  public static ByteIterator getByteIterator(byte[] value) {
    return new ByteArrayByteIterator(value);
  }
}
