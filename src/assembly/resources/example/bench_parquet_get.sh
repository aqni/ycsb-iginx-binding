#!/bin/sh

K=1024
M=$((1024 * K))

echo "row-group,page,file,implementation,throughput"

for max_row_group_size in $((256 * K)) $((2 * M)) $((16 * M)) $((128 * M))
do
  for max_page_size in $((4 * K)) $((64 * K)) $((1 * M))
  do
    for file_size in $((1*M)) $((4*M)) $((16*M)) $((64*M)) $((256*M))
    do
      for implementation in "duckdb-parquet" "parquet"
      do
        recordcount=$((file_size/K))
        slow=$((file_size/max_row_group_size))
        if ((slow == 0)); then
          slow=1
        fi
        operationcount=$((20480/slow))

        echo "==========================" >&2
        echo "with parquet.size.row_group=$max_row_group_size" >&2
        echo "with parquet.size.page=$max_page_size" >&2
        echo "with recordcount=$recordcount" >&2
        echo "with operationcount=$operationcount" >&2
        echo "with binding=$implementation" >&2

        printf "loading data... " >&2
        ./bin/ycsb.sh load $implementation -P workloads/workloadc \
          -p recordcount=$recordcount \
          -p parquet.size.row_group=$max_row_group_size \
          -p parquet.size.page=$max_page_size \
          >/dev/null 2>/dev/null
        echo "finished!" >&2

        echo "running benchmark... " >&2
        throughput=$(
          ./bin/ycsb.sh run $implementation -P workloads/workloadc \
            -p operationcount=$operationcount \
            | grep "\\[OVERALL\\], Throughput(ops/sec), " \
            | sed "s/\\[OVERALL\\], Throughput(ops\\/sec), //g"
        )

        echo "$max_row_group_size,$max_page_size,$file_size,$implementation,$throughput"
      done
    done
  done
done