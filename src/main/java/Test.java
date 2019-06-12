import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.InternalRow;
public class Test {


// Remove line numbers comments: /\* \d{3} \*/
//Found 1 WholeStageCodegen subtrees.
//== Subtree 1 / 1 ==
//*(1) Project [(if ((isnull(age#11) || isnull(education_num#15))) null else UDF(age#11, education_num#15) + if ((isnull(age#11) || isnull(education_num#15))) null else UDF(age#11, education_num#15)) AS (UDF(age, education_num) + UDF(age, education_num))#46, if ((isnull(age#11) || isnull(education_num#15))) null else UDF(age#11, education_num#15) AS UDF(age, education_num)#47]
//+- *(1) FileScan csv [age#11,education_num#15] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Applications/khiops/samples/Adult/Adult.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<age:int,education_num:int>

    //Generated code:
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


        private void project_doConsume_0(InternalRow scan_row_0, int project_expr_0_0, boolean project_exprIsNull_0_0, int project_expr_1_0, boolean project_exprIsNull_1_0) throws java.io.IOException, SparkException {

            boolean project_isNull_0 = true;

            int project_value_0 = -1;

            boolean project_value_2 = true;


            if (!project_exprIsNull_0_0) {

                project_value_2 = project_exprIsNull_1_0;

            }

            boolean project_isNull_1 = false;

            int project_value_1 = -1;

            if (!false && project_value_2) {

                project_isNull_1 = true;

                project_value_1 = -1;

            } else {

                Object project_arg_0 = project_exprIsNull_0_0 ? null : ((scala.Function1[]) references[1] /* converters */)[0].apply(project_expr_0_0);

                Object project_arg_1 = project_exprIsNull_1_0 ? null : ((scala.Function1[]) references[1] /* converters */)[1].apply(project_expr_1_0);


                Integer project_result_0 = null;

                try {

                    project_result_0 = (Integer) ((scala.Function1[]) references[1] /* converters */)[2].apply(((scala.Function2) references[3] /* udf */).apply(project_arg_0, project_arg_1));

                } catch (Exception e) {

                    throw new org.apache.spark.SparkException(((java.lang.String) references[2] /* errMsg */), e);

                }


                boolean project_isNull_8 = project_result_0 == null;

                int project_value_8 = -1;

                if (!project_isNull_8) {

                    project_value_8 = project_result_0;

                }

                project_isNull_1 = project_isNull_8;

                project_value_1 = project_value_8;

            }

            if (!project_isNull_1) {

                boolean project_value_12 = true;


                if (!project_exprIsNull_0_0) {

                    project_value_12 = project_exprIsNull_1_0;

                }

                boolean project_isNull_11 = false;

                int project_value_11 = -1;

                if (!false && project_value_12) {

                    project_isNull_11 = true;

                    project_value_11 = -1;

                } else {

                    Object project_arg_2 = project_exprIsNull_0_0 ? null : ((scala.Function1[]) references[4] /* converters */)[0].apply(project_expr_0_0);

                    Object project_arg_3 = project_exprIsNull_1_0 ? null : ((scala.Function1[]) references[4] /* converters */)[1].apply(project_expr_1_0);


                    Integer project_result_1 = null;

                    try {

                        project_result_1 = (Integer) ((scala.Function1[]) references[4] /* converters */)[2].apply(((scala.Function2) references[6] /* udf */).apply(project_arg_2, project_arg_3));

                    } catch (Exception e) {

                        throw new org.apache.spark.SparkException(((java.lang.String) references[5] /* errMsg */), e);

                    }


                    boolean project_isNull_18 = project_result_1 == null;

                    int project_value_18 = -1;

                    if (!project_isNull_18) {

                        project_value_18 = project_result_1;

                    }

                    project_isNull_11 = project_isNull_18;

                    project_value_11 = project_value_18;

                }

                if (!project_isNull_11) {

                    project_isNull_0 = false; // resultCode could change nullability.

                    project_value_0 = project_value_1 + project_value_11; // CUSTOMCOM: sum


                }


            }

            boolean project_value_22 = true;


            if (!project_exprIsNull_0_0) {

                project_value_22 = project_exprIsNull_1_0;

            }

            boolean project_isNull_21 = false;

            int project_value_21 = -1;

            if (!false && project_value_22) {

                project_isNull_21 = true;

                project_value_21 = -1;

            } else {

                Object project_arg_4 = project_exprIsNull_0_0 ? null : ((scala.Function1[]) references[7] /* converters */)[0].apply(project_expr_0_0);

                Object project_arg_5 = project_exprIsNull_1_0 ? null : ((scala.Function1[]) references[7] /* converters */)[1].apply(project_expr_1_0);


                Integer project_result_2 = null;

                try {

                    project_result_2 = (Integer) ((scala.Function1[]) references[7] /* converters */)[2].apply(((scala.Function2) references[9] /* udf */).apply(project_arg_4, project_arg_5));

                } catch (Exception e) {

                    throw new org.apache.spark.SparkException(((java.lang.String) references[8] /* errMsg */), e);

                }


                boolean project_isNull_28 = project_result_2 == null;

                int project_value_28 = -1;

                if (!project_isNull_28) {

                    project_value_28 = project_result_2;

                }

                project_isNull_21 = project_isNull_28;

                project_value_21 = project_value_28;

            }

            project_mutableStateArray_0[0].reset();


            project_mutableStateArray_0[0].zeroOutNullBytes();


            if (project_isNull_0) {

                project_mutableStateArray_0[0].setNullAt(0);

            } else {

                project_mutableStateArray_0[0].write(0, project_value_0);

            }


            if (project_isNull_21) {

                project_mutableStateArray_0[0].setNullAt(1);

            } else {

                project_mutableStateArray_0[0].write(1, project_value_21);

            }

            append((project_mutableStateArray_0[0].getRow()));  // CUSTOMCOM:  append is method from BufferedRowIterator


        }


        protected void processNext() throws java.io.IOException {

            while (scan_mutableStateArray_0[0].hasNext()) {

                InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();

                ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);

                boolean scan_isNull_0 = scan_row_0.isNullAt(0);

                int scan_value_0 = scan_isNull_0 ?
                        -1 : (scan_row_0.getInt(0));

                boolean scan_isNull_1 = scan_row_0.isNullAt(1);

                int scan_value_1 = scan_isNull_1 ?
                        -1 : (scan_row_0.getInt(1));


                try {
                    project_doConsume_0(scan_row_0, scan_value_0, scan_isNull_0, scan_value_1, scan_isNull_1);
                } catch (SparkException e) {
                    e.printStackTrace();
                }

                if (shouldStop()) return;
            }
        }
    }
}