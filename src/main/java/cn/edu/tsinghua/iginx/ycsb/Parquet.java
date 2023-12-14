package cn.edu.tsinghua.iginx.ycsb;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.IteratorScanner;
import cn.edu.tsinghua.iginx.parquet.db.Scanner;
import cn.edu.tsinghua.iginx.parquet.entity.Constants;
import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import cn.edu.tsinghua.iginx.parquet.entity.Range;
import cn.edu.tsinghua.iginx.parquet.io.Storer;
import cn.edu.tsinghua.iginx.parquet.io.parquet.impl.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.impl.IParquetWriter;
import cn.edu.tsinghua.iginx.parquet.io.parquet.impl.IRecord;
import cn.edu.tsinghua.iginx.parquet.tools.FilterRangeUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import cn.edu.tsinghua.iginx.parquet.db.WriteBuffer;
import site.ycsb.workloads.CoreWorkload;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class Parquet extends DB {

    private static final Logger logger = LoggerFactory.getLogger(Parquet.class);

    private final WriteBuffer<Long, String, Object> writeBuffer = new WriteBuffer<>();

    public static final String DUMP_FILE_PATH = "parquet.path.dump";

    public static final String DUMP_FILE_PATH_DEFAULT = "dump.parquet";

    public static final String READ_FILE_PATH = "parquet.path.read";

    public static final String READ_FILE_PATH_DEFAULT = "dump.parquet";

    public static final String ROW_GROUP_SIZE = "parquet.size.row_group";

    public static final String ROW_GROUP_SIZE_DEFAULT = "1048567";

    public static final String PAGE_SIZE = "parquet.size.page";

    public static final String PAGE_SIZE_DEFAULT = "8388608";

    public static final String SCAN_ALL = "parquet.scan.all";

    public static final String SCAN_ALL_DEFAULT = "false";

    protected Path dumpPath;

    protected Path readPath;

    protected int fieldCount;

    protected String fieldNamePrefix;

    protected String tableName;

    protected long rowGroupSize;

    protected int pageSize;

    protected boolean scanAll;

    static {
        org.apache.log4j.Logger.getLogger("org.apache.parquet.hadoop.InternalParquetRecordReader").setLevel(org.apache.log4j.Level.ERROR);
        org.apache.log4j.Logger.getLogger("org.apache.parquet.filter2.compat.FilterCompat").setLevel(org.apache.log4j.Level.ERROR);
    }

    @Override
    public void init() throws DBException {
        super.init();
        try {
            String dumpPathString = getProperties().getProperty(DUMP_FILE_PATH, DUMP_FILE_PATH_DEFAULT);
            dumpPath = Paths.get(dumpPathString);
            String readPathString = getProperties().getProperty(READ_FILE_PATH, READ_FILE_PATH_DEFAULT);
            readPath = Paths.get(readPathString);
            String fieldCountString = getProperties().getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT);
            fieldCount = Integer.parseInt(fieldCountString);
            fieldNamePrefix = getProperties().getProperty(CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
            tableName = getProperties().getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
            String rowGroupSizeString = getProperties().getProperty(ROW_GROUP_SIZE, ROW_GROUP_SIZE_DEFAULT);
            rowGroupSize = Long.parseLong(rowGroupSizeString);
            String pageSizeString = getProperties().getProperty(PAGE_SIZE, PAGE_SIZE_DEFAULT);
            pageSize = Integer.parseInt(pageSizeString);
            String scanAllString = getProperties().getProperty(SCAN_ALL, SCAN_ALL_DEFAULT);
            scanAll = Boolean.parseBoolean(scanAllString);
        } catch (Exception e) {
            throw new DBException("failed to init super", e);
        }
    }

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        List<Type> fields = new ArrayList<>();
        fields.add(Storer.getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
        for (int i = 0; i < fieldCount; i++) {
            fields.add(Storer.getParquetType(tableName + "." + fieldNamePrefix + i, DataType.BINARY, Type.Repetition.OPTIONAL));
        }
        MessageType parquetSchema = new MessageType(Constants.RECORD_FIELD_NAME, fields);
        try (Scanner<Long, Scanner<String, Object>> scanner = writeBuffer.scanRows(writeBuffer.fields(), writeBuffer.range())) {
            if (dumpPath.getParent() != null) {
                Files.createDirectories(dumpPath.getParent());
            }
            IParquetWriter.Builder builder = IParquetWriter.builder(dumpPath, parquetSchema).withPageSize(pageSize).withRowGroupSize(rowGroupSize);
            try (IParquetWriter writer = builder.build()) {
                while (scanner.iterate()) {
                    IRecord record = IParquetWriter.getRecord(parquetSchema, scanner.key(), scanner.value());
                    writer.write(record);
                }
            }
        } catch (Exception e) {
            throw new DBException("failed to dump into " + dumpPath, e);
        }
        logger.info("dumped into {}", dumpPath);
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        return scan(table, key, 0, fields, new Vector<>(1));
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        if (fields != null) {
            return Status.NOT_IMPLEMENTED;
        }
        long timestamp = CoreUtils.getTimestamp(startkey);
        long interval = recordcount;
        if (recordcount == 0) {
            interval = 1;
        } else if (scanAll) {
            timestamp = 0;
            interval = Long.MAX_VALUE;
        }

        return doScan(fields, result, timestamp, interval);
    }

    protected Status doScan(Set<String> fields, Vector<HashMap<String, ByteIterator>> result, long start, long interval) {
        if (fields != null) {
            return Status.NOT_IMPLEMENTED;
        }
        Filter filter = FilterRangeUtils.filterOf(new Range<>(start, start + interval));
        try (
            IParquetReader reader = IParquetReader.builder(readPath).filter(filter).build()) {
            MessageType parquetSchema = reader.getSchema();
            for (IRecord record = reader.read(); record != null; record = reader.read()) {
                HashMap<String, ByteIterator> map = new HashMap<>();
                for (Map.Entry<Integer, Object> entry : record) {
                    String fieldName = parquetSchema.getFieldName(entry.getKey());
                    if (fieldName.equals(Constants.KEY_FIELD_NAME)) {
                        continue;
                    }
                    String ycsbFieldName = CoreUtils.getFieldName(fieldName);
                    ByteIterator iterator = CoreUtils.getByteIterator((byte[]) entry.getValue());
                    map.put(ycsbFieldName, iterator);
                }
                result.add(map);
            }
            if (result.isEmpty()) {
                return Status.NOT_FOUND;
            }
            return Status.OK;
        } catch (
                Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        Map<Long, Map<String, Object>> rows = new HashMap<>();
        long timestamp = CoreUtils.getTimestamp(key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            String fieldName = table + "." + entry.getKey();
            Object value = CoreUtils.getValue(entry.getValue());
            Map<String, Object> row = rows.computeIfAbsent(timestamp, k -> new HashMap<>());
            row.put(fieldName, value);
        }
        Map<Long, Scanner<String, Object>> scannerMap = new HashMap<>();
        for (Map.Entry<Long, Map<String, Object>> entry : rows.entrySet()) {
            scannerMap.put(entry.getKey(), new IteratorScanner<>(entry.getValue().entrySet().iterator()));
        }
        try (Scanner<Long, Scanner<String, Object>> scanners = new IteratorScanner<>(scannerMap.entrySet().iterator())) {
            writeBuffer.putRows(scanners);
        } catch (NativeStorageException e) {
            logger.error("failed to put rows", e);
            return Status.ERROR;
        }
        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        return Status.NOT_IMPLEMENTED;
    }
}
