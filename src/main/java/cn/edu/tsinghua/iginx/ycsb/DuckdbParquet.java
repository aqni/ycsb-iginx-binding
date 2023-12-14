package cn.edu.tsinghua.iginx.ycsb;

import cn.edu.tsinghua.iginx.parquet.entity.Constants;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.sql.*;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

public class DuckdbParquet extends Parquet {

    private static final Logger logger = LoggerFactory.getLogger(DuckdbParquet.class);

    public static final String DUCKDB_CLOSED_PER_READ = "parquet.duckdb.closed_per_read";

    public static final String UCKDB_CLOSED_PER_READ_DEFAULT = "false";

    public static final String DUCKDB_ENABLE_OBJECT_CACHE = "parquet.duckdb.enable_object_cache";

    public static final String DUCKDB_ENABLE_OBJECT_CACHE_DEFAULT = "false";

    public static final String DUCKDB_LOAD_ONLY = "parquet.duckdb.load_only";

    public static final String DUCKDB_LOAD_ONLY_DEFAULT = "false";

    private Connection conn = null;

    private boolean loadOnly = false;

    @Override
    public void init() throws DBException {
        super.init();
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            String closedPerRead = getProperties().getProperty(DUCKDB_CLOSED_PER_READ, UCKDB_CLOSED_PER_READ_DEFAULT);
            boolean isClosedPerRead = Boolean.parseBoolean(closedPerRead);
            if (!isClosedPerRead) {
                conn = createDuckdbConnection();
            }
            String enableObjectCache = getProperties().getProperty(DUCKDB_ENABLE_OBJECT_CACHE, DUCKDB_ENABLE_OBJECT_CACHE_DEFAULT);
            boolean isEnableObjectCache = Boolean.parseBoolean(enableObjectCache);
            if (conn != null) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(String.format("SET enable_object_cache = %b;", isEnableObjectCache));
                }
            }
            String loadOnlyString = getProperties().getProperty(DUCKDB_LOAD_ONLY, DUCKDB_LOAD_ONLY_DEFAULT);
            this.loadOnly = Boolean.parseBoolean(loadOnlyString);
        } catch (Exception e) {
            throw new DBException("failed to init super", e);
        }
    }

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        try {
            conn.close();
        } catch (SQLException e) {
            logger.error("failed to close connection", e);
            throw new DBException("failed to close connection", e);
        }
    }

    @Override
    protected Status doScan(Set<String> fields, Vector<HashMap<String, ByteIterator>> result, long start, long interval) {
        if (fields != null) {
            return Status.NOT_IMPLEMENTED;
        }
        try (Connection conn = getDuckdbConnection()) {
            String sql = String.format("SELECT * FROM read_parquet('%s',binary_as_string=true) WHERE \"*\" >= %d AND \"*\" < %d;", readPath.toString(), start, start + interval);
            if (loadOnly) {
                sql = "DROP TABLE IF EXISTS test; CREATE TABLE test AS " + sql;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                try (ResultSet rs = stmt.getResultSet()) {
                    if(rs != null) {
                        ResultSetMetaData rsMetaData = rs.getMetaData();
                        while (rs.next()) {
                            HashMap<String, ByteIterator> map = new HashMap<>();
                            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                                String parquetFieldName = rsMetaData.getColumnName(i);
                                if (parquetFieldName.equals(Constants.KEY_FIELD_NAME)) {
                                    continue;
                                }
                                String fieldName = CoreUtils.getFieldName(parquetFieldName);
                                Object value = rs.getObject(i);
                                if (value == null) {
                                    continue;
                                }
                                ByteIterator byteIterator = CoreUtils.getByteIterator(((String) value).getBytes());
                                map.put(fieldName, byteIterator);
                            }
                            result.add(map);
                        }
                    }
                }
            }
            if (result.isEmpty()) {
                return Status.NOT_FOUND;
            }
            return Status.OK;
        } catch (Exception e) {
            logger.error("failed to scan", e);
            return Status.ERROR;
        }
    }

    private Connection getDuckdbConnection() throws SQLException {
        if (conn != null) {
            return ((DuckDBConnection) conn).duplicate();
        }
        return createDuckdbConnection();
    }

    private static Connection createDuckdbConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:duckdb:");
    }
}
