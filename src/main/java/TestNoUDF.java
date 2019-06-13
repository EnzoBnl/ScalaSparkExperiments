import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.InternalRow;

public class TestNoUDF {


    // Remove line numbers comments: /\* \d{3} \*/
//    Found 1 WholeStageCodegen subtrees.
//            == Subtree 1 / 1 ==
//            *(1) Project [(cast(cast((cast(age#11 as double) / cast((age#11 * 2) as double)) as int) as double) + (cast(age#11 as double) / cast((age#11 * 2) as double))) AS (CAST(CAST((CAST(age AS DOUBLE) / CAST((age * 2) AS DOUBLE)) AS INT) AS DOUBLE) + (CAST(age AS DOUBLE) / CAST((age * 2) AS DOUBLE)))#51, (cast(age#11 as double) / cast((age#11 * 2) as double)) AS (CAST(age AS DOUBLE) / CAST((age * 2) AS DOUBLE))#50]
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
            boolean project_isNull_0 = true;
            double project_value_0 = -1.0;
            boolean project_isNull_7 = true;
            int project_value_7 = -1;

            if (!project_exprIsNull_0_0) {
                project_isNull_7 = false; // resultCode could change nullability.
                project_value_7 = project_expr_0_0 * 2;

            }
            boolean project_isNull_6 = project_isNull_7;
            double project_value_6 = -1.0;
            if (!project_isNull_7) {
                project_value_6 = (double) project_value_7;
            }
            boolean project_isNull_3 = false;
            double project_value_3 = -1.0;
            if (project_isNull_6 || project_value_6 == 0) {
                project_isNull_3 = true;
            } else {
                boolean project_isNull_4 = project_exprIsNull_0_0;
                double project_value_4 = -1.0;
                if (!project_exprIsNull_0_0) {
                    project_value_4 = (double) project_expr_0_0;
                }
                if (project_isNull_4) {
                    project_isNull_3 = true;
                } else {
                    project_value_3 = (double) (project_value_4 / project_value_6);
                }
            }
            boolean project_isNull_2 = project_isNull_3;
            int project_value_2 = -1;
            if (!project_isNull_3) {
                project_value_2 = (int) project_value_3;
            }
            boolean project_isNull_1 = project_isNull_2;
            double project_value_1 = -1.0;
            if (!project_isNull_2) {
                project_value_1 = (double) project_value_2;
            }
            if (!project_isNull_1) {
                boolean project_isNull_14 = true;
                int project_value_14 = -1;

                if (!project_exprIsNull_0_0) {
                    project_isNull_14 = false; // resultCode could change nullability.
                    project_value_14 = project_expr_0_0 * 2;

                }
                boolean project_isNull_13 = project_isNull_14;
                double project_value_13 = -1.0;
                if (!project_isNull_14) {
                    project_value_13 = (double) project_value_14;
                }
                boolean project_isNull_10 = false;
                double project_value_10 = -1.0;
                if (project_isNull_13 || project_value_13 == 0) {
                    project_isNull_10 = true;
                } else {
                    boolean project_isNull_11 = project_exprIsNull_0_0;
                    double project_value_11 = -1.0;
                    if (!project_exprIsNull_0_0) {
                        project_value_11 = (double) project_expr_0_0;
                    }
                    if (project_isNull_11) {
                        project_isNull_10 = true;
                    } else {
                        project_value_10 = (double) (project_value_11 / project_value_13);
                    }
                }
                if (!project_isNull_10) {
                    project_isNull_0 = false; // resultCode could change nullability.
                    project_value_0 = project_value_1 + project_value_10;

                }

            }
            boolean project_isNull_21 = true;
            int project_value_21 = -1;

            if (!project_exprIsNull_0_0) {
                project_isNull_21 = false; // resultCode could change nullability.
                project_value_21 = project_expr_0_0 * 2;

            }
            boolean project_isNull_20 = project_isNull_21;
            double project_value_20 = -1.0;
            if (!project_isNull_21) {
                project_value_20 = (double) project_value_21;
            }
            boolean project_isNull_17 = false;
            double project_value_17 = -1.0;
            if (project_isNull_20 || project_value_20 == 0) {
                project_isNull_17 = true;
            } else {
                boolean project_isNull_18 = project_exprIsNull_0_0;
                double project_value_18 = -1.0;
                if (!project_exprIsNull_0_0) {
                    project_value_18 = (double) project_expr_0_0;
                }
                if (project_isNull_18) {
                    project_isNull_17 = true;
                } else {
                    project_value_17 = (double) (project_value_18 / project_value_20);
                }
            }
            project_mutableStateArray_0[0].reset();

            project_mutableStateArray_0[0].zeroOutNullBytes();

            if (project_isNull_0) {
                project_mutableStateArray_0[0].setNullAt(0);
            } else {
                project_mutableStateArray_0[0].write(0, project_value_0);
            }

            if (project_isNull_17) {
                project_mutableStateArray_0[0].setNullAt(1);
            } else {
                project_mutableStateArray_0[0].write(1, project_value_17);
            }
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