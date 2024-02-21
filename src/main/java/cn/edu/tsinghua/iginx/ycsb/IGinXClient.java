package cn.edu.tsinghua.iginx.ycsb;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;
import java.util.stream.Collectors;

public class IGinXClient extends DB {

  public static final String HOST_PROPERTY = "iginx.host";
  public static final String HOST_PROPERTY_DEFAULT = "127.0.0.1";
  public static final String PORT_PROPERTY = "iginx.port";
  public static final String PORT_PROPERTY_DEFAULT = "6888";
  public static final String USER_PROPERTY = "iginx.user";
  public static final String USER_PROPERTY_DEFAULT = "root";
  public static final String PASSWORD_PROPERTY = "iginx.password";
  public static final String PASSWORD_PROPERTY_DEFAULT = "root";
  public static final String CLIENT_PROPERTY = "iginx.buffersize";
  public static final String CLIENT_PROPERTY_DEFAULT = "0";
  public static final String INSERT_BY_COLUMN = "iginx.insertbycolumn";
  public static final String INSERT_BY_COLUMN_DEFAULT = "false";
  private static final Logger logger = LoggerFactory.getLogger(IGinXClient.class);
  private final Map<Long, Map<String, byte[]>> buffer = new HashMap<>();
  private int clientBufferSize;
  private boolean insertByColumn;
  private Session session;
  private int bufferPoints = 0;

  private static Map<String, Integer> getIndexMap(List<String> pathList) {
    Map<String, Integer> pathIndexMap = new HashMap<>();
    for (int i = 0; i < pathList.size(); i++) {
      pathIndexMap.put(pathList.get(i), i);
    }
    return pathIndexMap;
  }

  @Override
  public void init() throws DBException {
    int port;
    try {
      port = Integer.parseInt(getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));
    } catch (NumberFormatException e) {
      throw new DBException("fail to parse `iginx.port`", e);
    }
    String host = getProperties().getProperty(HOST_PROPERTY, HOST_PROPERTY_DEFAULT);
    String user = getProperties().getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);
    String password = getProperties().getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);

    this.clientBufferSize = Integer.parseInt(getProperties().getProperty(CLIENT_PROPERTY, CLIENT_PROPERTY_DEFAULT));
    this.insertByColumn = Boolean.parseBoolean(getProperties().getProperty(INSERT_BY_COLUMN, INSERT_BY_COLUMN_DEFAULT));
    this.session = new Session(host, port, user, password);
    try {
      this.session.openSession();
    } catch (SessionException e) {
      throw new DBException("fail to open session", e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.session.closeSession();
    } catch (SessionException e) {
      throw new DBException("fail to close session", e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Vector<HashMap<String, ByteIterator>> resultVector = new Vector<>();
    Status status = scan(table, key, 1, fields, resultVector);
    if (!result.isEmpty()) {
      result.putAll(resultVector.get(0));
    }
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    long timestamp = CoreUtils.getTimestamp(startkey);
    List<String> pathList = new ArrayList<>();
    if (fields == null) {
      pathList.add(table + ".*");
    } else {
      for (String field : fields) {
        pathList.add(table + "." + field);
      }
    }
    try {
      SessionQueryDataSet res = session.queryData(pathList, timestamp, timestamp + recordcount);
      if (res.getKeys().length == 0) {
        logger.warn("no data found in [{},{}) of {}", timestamp, timestamp + recordcount, pathList);
        return Status.NOT_FOUND;
      }
      for (int i = 0; i < res.getKeys().length; i++) {
        List<Object> values = res.getValues().get(i);
        HashMap<String, ByteIterator> currentResult = new HashMap<>();
        for (int j = 0; j < res.getPaths().size(); j++) {
          String fieldName = CoreUtils.getFieldName(res.getPaths().get(j));
          ByteIterator iterator = CoreUtils.getByteIterator((byte[]) values.get(j));
          currentResult.put(fieldName, iterator);
        }
        result.add(currentResult);
      }
      return Status.OK;
    } catch (Exception e) {
      String message = String.format("fail to scan %s from %s to %s", fields, timestamp, timestamp + recordcount);
      logger.error(message, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    long timestamp = CoreUtils.getTimestamp(key);
    Map<String, byte[]> record = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String path = CoreUtils.getPath(table, entry.getKey());
      byte[] value = CoreUtils.getValue(entry.getValue());
      record.put(path, value);
    }

    return insert(timestamp, record);
  }

  @Override
  public Status delete(String table, String key) {
    long timestamp = CoreUtils.getTimestamp(key);
    try {
      session.deleteDataInColumn(table + ".*", timestamp, timestamp + 1);
      return Status.OK;
    } catch (Exception e) {
      String message = String.format("fail to delete %s at %s", table, timestamp);
      logger.error(message, e);
      return Status.ERROR;
    }
  }

  private Status insert(long timestamp, Map<String, byte[]> record) {
    buffer.computeIfAbsent(timestamp, k -> new HashMap<>()).putAll(record);
    bufferPoints += record.size();

    if (bufferPoints < clientBufferSize) {
      return Status.OK;
    }

    try {
      return insertBuffer();
    } catch (Exception e) {
      logger.error("fail to insert", e);
      return Status.ERROR;
    } finally {
      buffer.clear();
      bufferPoints = 0;
    }
  }

  private Status insertBuffer() throws SessionException, ExecutionException {
    List<String> pathList = buffer.values().stream().map(Map::keySet)
        .flatMap(Collection::stream).distinct().collect(Collectors.toList());
    List<DataType> dataTypeList = pathList.stream().map(p -> DataType.BINARY).collect(Collectors.toList());
    long[] timestamps = buffer.keySet().stream().mapToLong(Long::longValue).toArray();

    if (insertByColumn) {
      return insertColumn(pathList, dataTypeList, timestamps);
    } else {
      return insertRow(pathList, dataTypeList, timestamps);
    }
  }

  private Status insertColumn(List<String> pathList, List<DataType> dataTypeList, long[] timestamps) throws SessionException, ExecutionException {
    Object[] valuesList = new Object[pathList.size()];
    for (int i = 0; i < pathList.size(); i++) {
      valuesList[i] = new Object[timestamps.length];
    }

    Map<String, Integer> pathIndexMap = getIndexMap(pathList);
    for (int i = 0; i < timestamps.length; i++) {
      Map<String, byte[]> record = buffer.get(timestamps[i]);
      for (Map.Entry<String, byte[]> entry : record.entrySet()) {
        int index = pathIndexMap.get(entry.getKey());
        ((Object[]) valuesList[index])[i] = entry.getValue();
      }
    }

    session.insertColumnRecords(pathList, timestamps, valuesList, dataTypeList, null);
    return Status.OK;
  }

  private Status insertRow(List<String> pathList, List<DataType> dataTypeList, long[] timestamps) throws SessionException, ExecutionException {
    Object[] valuesList = new Object[timestamps.length];

    Map<String, Integer> pathIndexMap = getIndexMap(pathList);
    for (int i = 0; i < timestamps.length; i++) {
      Map<String, byte[]> record = buffer.get(timestamps[i]);
      Object[] values = new Object[pathList.size()];
      for (Map.Entry<String, byte[]> entry : record.entrySet()) {
        int index = pathIndexMap.get(entry.getKey());
        values[index] = entry.getValue();
      }
      valuesList[i] = values;
    }

    session.insertRowRecords(pathList, timestamps, valuesList, dataTypeList, null);
    return Status.OK;
  }

}
