#!/bin/sh

SCAN_ALL=true
LOAD_ONLY=true

K=1024
M=$((1024 * K))

echo "row-group,file,implementation,throughput"

for max_row_group_size in $((2 * M)) $((16 * M)) $((128 * M))
do
  for max_page_size in $((64 * K))
  do
    for file_size in $((1*M)) $((2*M)) $((4*M)) $((8*M)) $((16*M)) $((32*M)) $((64*M)) $((128*M)) $((256*M))
    do
      for implementation in "duckdb-parquet" "parquet"
      do
        recordcount=$((file_size/K))
        operationcount=$((1024*M/file_size))
        if ((operationcount == 0)); then
          operationcount=1
        fi

        echo "==========================" >&2
        echo "with parquet.size.row_group=$max_row_group_size" >&2
        echo "with parquet.size.page=$max_page_size" >&2
        echo "with recordcount=$recordcount" >&2
        echo "with operationcount=$operationcount" >&2
        echo "with binding=$implementation" >&2
        echo "with scan_all=$SCAN_ALL" >&2

        printf "loading data... " >&2
        ./bin/ycsb.sh load $implementation -P workloads/workloade \
          -p recordcount=$recordcount \
          -p parquet.size.row_group=$max_row_group_size \
          -p parquet.size.page=$max_page_size \
          >/dev/null 2>/dev/null
        echo "finished!" >&2

        echo "running benchmark... " >&2
        throughput=$(
          ./bin/ycsb.sh run $implementation -P workloads/workloade \
            -p operationcount=$operationcount \
            -p scanproportion=1 \
            -p insertproportion=0 \
            -p parquet.scan.all=$SCAN_ALL \
            -p parquet.duckdb.load_only=$LOAD_ONLY \
            | grep "\\[OVERALL\\], Throughput(ops/sec), " \
            | sed "s/\\[OVERALL\\], Throughput(ops\\/sec), //g"
        )

        echo "$max_row_group_size,$file_size,$implementation,$throughput"
      done
    done
  done
done