package cn.edu.tsinghua.iginx.ycsb;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.util.*;

public class Session extends DB {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    public static final String HOST_PROPERTY = "iginx.host";
    public static final String HOST_PROPERTY_DEFAULT = "127.0.0.1";
    public static final String PORT_PROPERTY = "iginx.port";
    public static final String PORT_PROPERTY_DEFAULT = "6888";
    public static final String USER_PROPERTY = "iginx.user";
    public static final String USER_PROPERTY_DEFAULT = "root";
    public static final String PASSWORD_PROPERTY = "iginx.password";
    public static final String PASSWORD_PROPERTY_DEFAULT = "root";

    private cn.edu.tsinghua.iginx.session.Session session = null;

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

        this.session = new cn.edu.tsinghua.iginx.session.Session(host, port, user, password);
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
        long[] keyList = new long[]{timestamp};
        List<String> pathList = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            String fieldName = entry.getKey();
            pathList.add(table + "." + fieldName);
            dataTypeList.add(DataType.BINARY);
            valueList.add(CoreUtils.getValue(entry.getValue()));
        }
        Object[] rowList = new Object[]{valueList.toArray()};
        try {
            session.insertRowRecords(pathList, keyList, rowList, dataTypeList, null);
            return Status.OK;
        } catch (Exception e) {
            String message = String.format("fail to insert %s at %s", values, timestamp);
            logger.error(message, e);
            return Status.ERROR;
        }
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
}
