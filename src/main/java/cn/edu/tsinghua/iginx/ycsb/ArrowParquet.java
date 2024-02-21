package cn.edu.tsinghua.iginx.ycsb;

import cn.edu.tsinghua.iginx.parquet.shared.Constants;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

public class ArrowParquet extends Parquet {

  private static final Logger logger = LoggerFactory.getLogger(ArrowParquet.class);

  private String uri;

  @Override
  public void init() throws DBException {
    super.init();
    uri = readPath.toUri().toString();
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
  }

  @Override
  protected Status doScan(Set<String> fields, Vector<HashMap<String, ByteIterator>> result, long start, long interval) {
    if (fields != null) {
      return Status.NOT_IMPLEMENTED;
    }
    ScanOptions options = new ScanOptions.Builder(2048).build();
    try (
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
            allocator, NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      scanner.schema();
      while (reader.loadNextBatch()) {
        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
          FieldVector keyVector = root.getVector(Constants.KEY_FIELD_NAME);
          for (int i = 0; i < root.getRowCount(); i++) {
            Object key = keyVector.getObject(i);
            if (!(key instanceof Long)) {
              throw new DBException("key is not long, but: " + key.getClass().getName());
            }
            if ((Long) key < start || (Long) key >= start + interval) {
              continue;
            }
            HashMap<String, ByteIterator> map = new HashMap<>();
            for (FieldVector fieldVector : root.getFieldVectors()) {
              String name = fieldVector.getName();
              if (name.equals(Constants.KEY_FIELD_NAME)) {
                continue;
              }
              Object value = fieldVector.getObject(i);
              if (!(value instanceof byte[])) {
                throw new DBException("value is not byte[], but: " + value.getClass().getName());
              }
              String ycsbFieldName = CoreUtils.getFieldName(name);
              ByteIterator iterator = CoreUtils.getByteIterator((byte[]) value);
              map.put(ycsbFieldName, iterator);
            }
            result.add(map);
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
}
