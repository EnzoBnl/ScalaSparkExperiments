import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

public class TestHashVarArg {
    // Remove line numbers comments: /\* \d{3} \*/
//    Found 1 WholeStageCodegen subtrees.
//            == Subtree 1 / 1 ==
//            *(1) Project [(hash(cast(age#11 as string), 42) + hash(cast(age#11 as string), 42)) AS (hash(CAST(age AS STRING)) + hash(CAST(age AS STRING)))#46, hash(cast(age#11 as string), 42) AS hash(CAST(age AS STRING))#47]
//            +- *(1) FileScan csv [age#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Applications/khiops/samples/Adult/Adult.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<age:int>
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

        private void project_doConsume_0(InternalRow scan_row_0, int project_expr_0_0, boolean project_exprIsNull_0_0) throws java.io.IOException {
            int project_value_1 = 42;
            boolean project_isNull_2 = project_exprIsNull_0_0;
            UTF8String project_value_2 = null;
            if (!project_exprIsNull_0_0) {
                project_value_2 = UTF8String.fromString(String.valueOf(project_expr_0_0));
            }
            if (!project_isNull_2) {
                project_value_1 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(project_value_2.getBaseObject(), project_value_2.getBaseOffset(), project_value_2.numBytes(), project_value_1);
            }
            int project_value_4 = 42;
            boolean project_isNull_5 = project_exprIsNull_0_0;
            UTF8String project_value_5 = null;
            if (!project_exprIsNull_0_0) {
                project_value_5 = UTF8String.fromString(String.valueOf(project_expr_0_0));
            }
            if (!project_isNull_5) {
                project_value_4 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(project_value_5.getBaseObject(), project_value_5.getBaseOffset(), project_value_5.numBytes(), project_value_4);
            }
            int project_value_0 = -1;
            project_value_0 = project_value_1 + project_value_4;
            int project_value_7 = 42; // TODO: WTF la seed est fixe (toujours 42), d'une comp
            boolean project_isNull_8 = project_exprIsNull_0_0;
            UTF8String project_value_8 = null;
            if (!project_exprIsNull_0_0) {
                project_value_8 = UTF8String.fromString(String.valueOf(project_expr_0_0));
            }
            if (!project_isNull_8) {
                project_value_7 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(project_value_8.getBaseObject(), project_value_8.getBaseOffset(), project_value_8.numBytes(), project_value_7);
            }
            project_mutableStateArray_0[0].reset();

            project_mutableStateArray_0[0].write(0, project_value_0);

            project_mutableStateArray_0[0].write(1, project_value_7);
            append((project_mutableStateArray_0[0].getRow()));

        }

        protected void processNext() throws java.io.IOException {
            while (scan_mutableStateArray_0[0].hasNext()) {
                InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
                boolean scan_isNull_0 = scan_row_0.isNullAt(0);
                int scan_value_0 = scan_isNull_0 ?
                        -1 : (scan_row_0.getInt(0));

                project_doConsume_0(scan_row_0, scan_value_0, scan_isNull_0);
                if (shouldStop()) return;
            }
        }

    }
}