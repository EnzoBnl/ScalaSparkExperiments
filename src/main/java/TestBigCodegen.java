import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

public class TestBigCodegen {
//    == Parsed Logical Plan ==
//    Join RightOuter, (id#18L = src#4L)
//            :- RepartitionByExpression [src#4L], 48
//            :  +- Project [src#0L AS src#4L, dst#1L AS dst#5L]
//            :     +- Relation[src#0L,dst#1L] csv
//+- RepartitionByExpression [id#18L], 48
//            +- Project [id#18L, 1 AS PR#23]
//            +- Union
//         :- Deduplicate [id#18L]
//            :  +- Project [src#4L AS id#18L]
//            :     +- RepartitionByExpression [src#4L], 48
//            :        +- Project [src#0L AS src#4L, dst#1L AS dst#5L]
//            :           +- Relation[src#0L,dst#1L] csv
//         +- Deduplicate [id#20L]
//            +- Project [dst#5L AS id#20L]
//            +- RepartitionByExpression [src#4L], 48
//            +- Project [src#0L AS src#4L, dst#1L AS dst#5L]
//            +- Relation[src#0L,dst#1L] csv
//
//== Analyzed Logical Plan ==
//    src: bigint, dst: bigint, id: bigint, PR: int
//    Join RightOuter, (id#18L = src#4L)
//            :- RepartitionByExpression [src#4L], 48
//            :  +- Project [src#0L AS src#4L, dst#1L AS dst#5L]
//            :     +- Relation[src#0L,dst#1L] csv
//+- RepartitionByExpression [id#18L], 48
//            +- Project [id#18L, 1 AS PR#23]
//            +- Union
//         :- Deduplicate [id#18L]
//            :  +- Project [src#4L AS id#18L]
//            :     +- RepartitionByExpression [src#4L], 48
//            :        +- Project [src#0L AS src#4L, dst#1L AS dst#5L]
//            :           +- Relation[src#0L,dst#1L] csv
//         +- Deduplicate [id#20L]
//            +- Project [dst#5L AS id#20L]
//            +- RepartitionByExpression [src#4L], 48
//            +- Project [src#0L AS src#4L, dst#1L AS dst#5L]
//            +- Relation[src#0L,dst#1L] csv
//
//== Optimized Logical Plan ==
//    Join RightOuter, (id#18L = src#4L)
//            :- Filter isnotnull(src#4L)
//:  +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//:        +- Exchange hashpartitioning(src#0L, 48)
//:           +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//            +- InMemoryRelation [id#18L, PR#23], StorageLevel(disk, memory, deserialized, 1 replicas)
//      +- Exchange hashpartitioning(id#18L, 48)
//         +- Union
//            :- *(2) HashAggregate(keys=[id#18L], functions=[], output=[id#18L, PR#23])
//            :  +- Exchange hashpartitioning(id#18L, 48)
//            :     +- *(1) HashAggregate(keys=[id#18L], functions=[], output=[id#18L])
//            :        +- *(1) Project [src#4L AS id#18L]
//            :           +- *(1) InMemoryTableScan [src#4L]
//            :                 +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//            :                       +- Exchange hashpartitioning(src#0L, 48)
//            :                          +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//            +- *(4) HashAggregate(keys=[id#20L], functions=[], output=[id#20L, PR#46])
//               +- Exchange hashpartitioning(id#20L, 48)
//                  +- *(3) HashAggregate(keys=[id#20L], functions=[], output=[id#20L])
//                     +- *(3) Project [dst#5L AS id#20L]
//            +- *(3) InMemoryTableScan [dst#5L]
//            +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//                                    +- Exchange hashpartitioning(src#0L, 48)
//                                       +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//
//== Physical Plan ==
//SortMergeJoin [src#4L], [id#18L], RightOuter
//:- *(1) Sort [src#4L ASC NULLS FIRST], false, 0
//:  +- *(1) Filter isnotnull(src#4L)
//:     +- *(1) InMemoryTableScan [src#4L, dst#5L], [isnotnull(src#4L)]
//:           +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//:                 +- Exchange hashpartitioning(src#0L, 48)
//:                    +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//+- *(2) Sort [id#18L ASC NULLS FIRST], false, 0
//   +- *(2) InMemoryTableScan [id#18L, PR#23]
//         +- InMemoryRelation [id#18L, PR#23], StorageLevel(disk, memory, deserialized, 1 replicas)
//               +- Exchange hashpartitioning(id#18L, 48)
//                  +- Union
//                     :- *(2) HashAggregate(keys=[id#18L], functions=[], output=[id#18L, PR#23])
//                     :  +- Exchange hashpartitioning(id#18L, 48)
//                     :     +- *(1) HashAggregate(keys=[id#18L], functions=[], output=[id#18L])
//                     :        +- *(1) Project [src#4L AS id#18L]
//                     :           +- *(1) InMemoryTableScan [src#4L]
//                     :                 +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//                     :                       +- Exchange hashpartitioning(src#0L, 48)
//                     :                          +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//                     +- *(4) HashAggregate(keys=[id#20L], functions=[], output=[id#20L, PR#46])
//                        +- Exchange hashpartitioning(id#20L, 48)
//                           +- *(3) HashAggregate(keys=[id#20L], functions=[], output=[id#20L])
//                              +- *(3) Project [dst#5L AS id#20L]
//                                 +- *(3) InMemoryTableScan [dst#5L]
//                                       +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//                                             +- Exchange hashpartitioning(src#0L, 48)
//                                                +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>

    //    Found 2 WholeStageCodegen subtrees.
//== Subtree 1 / 2 ==
//*(2) Sort [id#18L ASC NULLS FIRST], false, 0
//  +- *(2) InMemoryTableScan [id#18L, PR#23]
//    +- InMemoryRelation [id#18L, PR#23], StorageLevel(disk, memory, deserialized, 1 replicas)
//      +- Exchange hashpartitioning(id#18L, 48)
//        +- Union
//        :- *(2) HashAggregate(keys=[id#18L], functions=[], output=[id#18L, PR#23])
//        :  +- Exchange hashpartitioning(id#18L, 48)
//        :     +- *(1) HashAggregate(keys=[id#18L], functions=[], output=[id#18L])
//        :        +- *(1) Project [src#4L AS id#18L]
//        :           +- *(1) InMemoryTableScan [src#4L]
//        :                 +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//        :                       +- Exchange hashpartitioning(src#0L, 48)
//        :                          +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//        +- *(4) HashAggregate(keys=[id#20L], functions=[], output=[id#20L, PR#46])
//           +- Exchange hashpartitioning(id#20L, 48)
//              +- *(3) HashAggregate(keys=[id#20L], functions=[], output=[id#20L])
//                 +- *(3) Project [dst#5L AS id#20L]
//                    +- *(3) InMemoryTableScan [dst#5L]
//                          +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//                                +- Exchange hashpartitioning(src#0L, 48)
//                                   +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//
//    Generated code:
    public Object generate(Object[] references) {
        return new GeneratedIteratorForCodegenStage2(references);
    }

    // codegenStageId=2
    final class GeneratedIteratorForCodegenStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
        private Object[] references;
        private scala.collection.Iterator[] inputs;
        private boolean sort_needToSort_0;
        private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
        private org.apache.spark.executor.TaskMetrics sort_metrics_0;
        private scala.collection.Iterator<UnsafeRow> sort_sortedIter_0;
        private long inmemorytablescan_scanTime_0;
        private int inmemorytablescan_batchIdx_0;
        private org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[] inmemorytablescan_mutableStateArray_2 = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[2];
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] inmemorytablescan_mutableStateArray_3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];
        private org.apache.spark.sql.vectorized.ColumnarBatch[] inmemorytablescan_mutableStateArray_1 = new org.apache.spark.sql.vectorized.ColumnarBatch[1];
        private scala.collection.Iterator[] inmemorytablescan_mutableStateArray_0 = new scala.collection.Iterator[1];

        public GeneratedIteratorForCodegenStage2(Object[] references) {
            this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
            partitionIndex = index;
            this.inputs = inputs;
            sort_needToSort_0 = true;
            sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0]).createSorter();
            sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();

            inmemorytablescan_mutableStateArray_0[0] = inputs[0];
            inmemorytablescan_mutableStateArray_3[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);

        }

        private void inmemorytablescan_nextBatch_0() throws java.io.IOException {
            long getBatchStart = System.nanoTime();
            if (inmemorytablescan_mutableStateArray_0[0].hasNext()) {
                inmemorytablescan_mutableStateArray_1[0] = (org.apache.spark.sql.vectorized.ColumnarBatch) inmemorytablescan_mutableStateArray_0[0].next();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[1]).add(inmemorytablescan_mutableStateArray_1[0].numRows());
                inmemorytablescan_batchIdx_0 = 0;
                inmemorytablescan_mutableStateArray_2[0] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) inmemorytablescan_mutableStateArray_1[0].column(0);
                inmemorytablescan_mutableStateArray_2[1] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) inmemorytablescan_mutableStateArray_1[0].column(1);

            }
            inmemorytablescan_scanTime_0 += System.nanoTime() - getBatchStart;
        }

        private void sort_addToSorter_0() throws java.io.IOException {
            if (inmemorytablescan_mutableStateArray_1[0] == null) {
                inmemorytablescan_nextBatch_0();
            }
            while (inmemorytablescan_mutableStateArray_1[0] != null) {
                int inmemorytablescan_numRows_0 = inmemorytablescan_mutableStateArray_1[0].numRows();
                int inmemorytablescan_localEnd_0 = inmemorytablescan_numRows_0 - inmemorytablescan_batchIdx_0;
                for (int inmemorytablescan_localIdx_0 = 0; inmemorytablescan_localIdx_0 < inmemorytablescan_localEnd_0; inmemorytablescan_localIdx_0++) {
                    int inmemorytablescan_rowIdx_0 = inmemorytablescan_batchIdx_0 + inmemorytablescan_localIdx_0;
                    boolean inmemorytablescan_isNull_0 = inmemorytablescan_mutableStateArray_2[0].isNullAt(inmemorytablescan_rowIdx_0);
                    long inmemorytablescan_value_0 = inmemorytablescan_isNull_0 ? -1L : (inmemorytablescan_mutableStateArray_2[0].getLong(inmemorytablescan_rowIdx_0));
                    int inmemorytablescan_value_1 = inmemorytablescan_mutableStateArray_2[1].getInt(inmemorytablescan_rowIdx_0);
                    inmemorytablescan_mutableStateArray_3[0].reset();

                    inmemorytablescan_mutableStateArray_3[0].zeroOutNullBytes();

                    if (inmemorytablescan_isNull_0) {
                        inmemorytablescan_mutableStateArray_3[0].setNullAt(0);
                    } else {
                        inmemorytablescan_mutableStateArray_3[0].write(0, inmemorytablescan_value_0);
                    }

                    inmemorytablescan_mutableStateArray_3[0].write(1, inmemorytablescan_value_1);
                    sort_sorter_0.insertRow((UnsafeRow) (inmemorytablescan_mutableStateArray_3[0].getRow()));
                    // shouldStop check is eliminated
                }
                inmemorytablescan_batchIdx_0 = inmemorytablescan_numRows_0;
                inmemorytablescan_mutableStateArray_1[0] = null;
                inmemorytablescan_nextBatch_0();
            }
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[2]).add(inmemorytablescan_scanTime_0 / (1000 * 1000));
            inmemorytablescan_scanTime_0 = 0;

        }

        protected void processNext() throws java.io.IOException {
            if (sort_needToSort_0) {
                long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
                sort_addToSorter_0();
                sort_sortedIter_0 = sort_sorter_0.sort();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[5]).add(sort_sorter_0.getSortTimeNanos() / 1000000);
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[3]).add(sort_sorter_0.getPeakMemoryUsage());
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[4]).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
                sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
                sort_needToSort_0 = false;
            }

            while (sort_sortedIter_0.hasNext()) {
                UnsafeRow sort_outputRow_0 = (UnsafeRow) sort_sortedIter_0.next();

                append(sort_outputRow_0);

                if (shouldStop()) return;
            }
        }

    }

    //== Subtree 2 / 2 ==
//            *(1) Sort [src#4L ASC NULLS FIRST], false, 0
//            +- *(1) Filter isnotnull(src#4L)
//   +- *(1) InMemoryTableScan [src#4L, dst#5L], [isnotnull(src#4L)]
//            +- InMemoryRelation [src#4L, dst#5L], StorageLevel(disk, memory, deserialized, 1 replicas)
//               +- Exchange hashpartitioning(src#0L, 48)
//                  +- *(1) FileScan csv [src#0L,dst#1L] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Data/celineengsrcdst.edges], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<src:bigint,dst:bigint>
//
//    Generated code:
    public Object generate2(Object[] references) {
        return new GeneratedIteratorForCodegenStage1(references);
    }

    // codegenStageId=1
    final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
        private Object[] references;
        private scala.collection.Iterator[] inputs;
        private boolean sort_needToSort_0;
        private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
        private org.apache.spark.executor.TaskMetrics sort_metrics_0;
        private scala.collection.Iterator<UnsafeRow> sort_sortedIter_0;
        private long inmemorytablescan_scanTime_0;
        private int inmemorytablescan_batchIdx_0;
        private org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[] inmemorytablescan_mutableStateArray_2 = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[2];
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] inmemorytablescan_mutableStateArray_3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];
        private org.apache.spark.sql.vectorized.ColumnarBatch[] inmemorytablescan_mutableStateArray_1 = new org.apache.spark.sql.vectorized.ColumnarBatch[1];
        private scala.collection.Iterator[] inmemorytablescan_mutableStateArray_0 = new scala.collection.Iterator[1];

        public GeneratedIteratorForCodegenStage1(Object[] references) {
            this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
            partitionIndex = index;
            this.inputs = inputs;
            sort_needToSort_0 = true;
            sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0]).createSorter();
            sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();

            inmemorytablescan_mutableStateArray_0[0] = inputs[0];
            inmemorytablescan_mutableStateArray_3[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
            inmemorytablescan_mutableStateArray_3[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);

        }

        private void inmemorytablescan_nextBatch_0() throws java.io.IOException {
            long getBatchStart = System.nanoTime();
            if (inmemorytablescan_mutableStateArray_0[0].hasNext()) {
                inmemorytablescan_mutableStateArray_1[0] = (org.apache.spark.sql.vectorized.ColumnarBatch) inmemorytablescan_mutableStateArray_0[0].next();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[1]).add(inmemorytablescan_mutableStateArray_1[0].numRows());
                inmemorytablescan_batchIdx_0 = 0;
                inmemorytablescan_mutableStateArray_2[0] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) inmemorytablescan_mutableStateArray_1[0].column(0);
                inmemorytablescan_mutableStateArray_2[1] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) inmemorytablescan_mutableStateArray_1[0].column(1);

            }
            inmemorytablescan_scanTime_0 += System.nanoTime() - getBatchStart;
        }

        private void sort_addToSorter_0() throws java.io.IOException {
            if (inmemorytablescan_mutableStateArray_1[0] == null) {
                inmemorytablescan_nextBatch_0();
            }
            while (inmemorytablescan_mutableStateArray_1[0] != null) {
                int inmemorytablescan_numRows_0 = inmemorytablescan_mutableStateArray_1[0].numRows();
                int inmemorytablescan_localEnd_0 = inmemorytablescan_numRows_0 - inmemorytablescan_batchIdx_0;
                for (int inmemorytablescan_localIdx_0 = 0; inmemorytablescan_localIdx_0 < inmemorytablescan_localEnd_0; inmemorytablescan_localIdx_0++) {
                    int inmemorytablescan_rowIdx_0 = inmemorytablescan_batchIdx_0 + inmemorytablescan_localIdx_0;
                    do {
                        boolean inmemorytablescan_isNull_0 = inmemorytablescan_mutableStateArray_2[0].isNullAt(inmemorytablescan_rowIdx_0);
                        long inmemorytablescan_value_0 = inmemorytablescan_isNull_0 ? -1L : (inmemorytablescan_mutableStateArray_2[0].getLong(inmemorytablescan_rowIdx_0));

                        if (!(!inmemorytablescan_isNull_0)) continue;

                        ((org.apache.spark.sql.execution.metric.SQLMetric) references[3]).add(1);

                        boolean inmemorytablescan_isNull_1 = inmemorytablescan_mutableStateArray_2[1].isNullAt(inmemorytablescan_rowIdx_0);
                        long inmemorytablescan_value_1 = inmemorytablescan_isNull_1 ? -1L : (inmemorytablescan_mutableStateArray_2[1].getLong(inmemorytablescan_rowIdx_0));
                        inmemorytablescan_mutableStateArray_3[1].reset();

                        inmemorytablescan_mutableStateArray_3[1].zeroOutNullBytes();

                        inmemorytablescan_mutableStateArray_3[1].write(0, inmemorytablescan_value_0);

                        if (inmemorytablescan_isNull_1) {
                            inmemorytablescan_mutableStateArray_3[1].setNullAt(1);
                        } else {
                            inmemorytablescan_mutableStateArray_3[1].write(1, inmemorytablescan_value_1);
                        }
                        sort_sorter_0.insertRow((UnsafeRow) (inmemorytablescan_mutableStateArray_3[1].getRow()));

                    } while (false);
                    // shouldStop check is eliminated
                }
                inmemorytablescan_batchIdx_0 = inmemorytablescan_numRows_0;
                inmemorytablescan_mutableStateArray_1[0] = null;
                inmemorytablescan_nextBatch_0();
            }
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[2]).add(inmemorytablescan_scanTime_0 / (1000 * 1000));
            inmemorytablescan_scanTime_0 = 0;

        }

        protected void processNext() throws java.io.IOException {
            if (sort_needToSort_0) {
                long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
                sort_addToSorter_0();
                sort_sortedIter_0 = sort_sorter_0.sort();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[6]).add(sort_sorter_0.getSortTimeNanos() / 1000000);
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[4]).add(sort_sorter_0.getPeakMemoryUsage());
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[5]).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
                sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
                sort_needToSort_0 = false;
            }

            while (sort_sortedIter_0.hasNext()) {
                UnsafeRow sort_outputRow_0 = (UnsafeRow) sort_sortedIter_0.next();

                append(sort_outputRow_0);

                if (shouldStop()) return;
            }
        }

    }
}
