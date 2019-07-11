import org.apache.spark.sql.catalyst.InternalRow;

public class TestHashFixArg {
    // Remove line numbers comments: /\* \d{3} \*/

    //    Found 1 WholeStageCodegen subtrees.
//            == Subtree 1 / 1 ==
//            *(1) Project [2108510444 AS (hash(bla) + hash(bla))#50, 1054255222 AS hash(bla)#51]
//            +- *(1) FileScan csv [] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Applications/khiops/samples/Adult/Adult.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<>
//
//    Generated code:
    public Object generate(Object[] references) {
        return new GeneratedIteratorForCodegenStage1(references);
    }

    // codegenStageId=1
    final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
        private Object[] references;
        private scala.collection.Iterator[] inputs;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] project_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];
        private scala.collection.Iterator[] scan_mutableStateArray_0 = new scala.collection.Iterator[1];

        public GeneratedIteratorForCodegenStage1(Object[] references) {
            this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
            partitionIndex = index;
            this.inputs = inputs;
            scan_mutableStateArray_0[0] = inputs[0];
            project_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);

        }

        private void project_doConsume_0(InternalRow scan_row_0) throws java.io.IOException {
            project_mutableStateArray_0[0].reset();

            project_mutableStateArray_0[0].write(0, 2108510444);

            project_mutableStateArray_0[0].write(1, 1054255222);
            append((project_mutableStateArray_0[0].getRow()));
        }

        protected void processNext() throws java.io.IOException {
            while (scan_mutableStateArray_0[0].hasNext()) {
                InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
                project_doConsume_0(scan_row_0);
                if (shouldStop()) return;
            }
        }

    }
}