///* 001 */ public Object generate(Object[]references){
//        /* 002 */   return new GeneratedIteratorForCodegenStage1(references);
//        /* 003 */ }
//
///* 004 */
///* 005 */ // codegenStageId=1
///* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
//    /* 007 */   private Object[] references;
//    /* 008 */   private scala.collection.Iterator[] inputs;
//    /* 009 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] project_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];
//    /* 010 */   private scala.collection.Iterator[] scan_mutableStateArray_0 = new scala.collection.Iterator[1];
//
//    /* 011 */
//    /* 012 */
//    public GeneratedIteratorForCodegenStage1(Object[] references) {
//        /* 013 */
//        this.references = references;
//        /* 014 */
//    }
//
//    /* 015 */
//    /* 016 */
//    public void init(int index, scala.collection.Iterator[] inputs) {
//        /* 017 */
//        partitionIndex = index;
//        /* 018 */
//        this.inputs = inputs;
//        /* 019 */
//        scan_mutableStateArray_0[0] = inputs[0];
//        /* 020 */
//        project_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
//        /* 021 */
//        /* 022 */
//    }
//
//    /* 023 */
//    /* 024 */
//    private void project_doConsume_0(InternalRow scan_row_0, int project_expr_0_0, boolean project_exprIsNull_0_0, int project_expr_1_0, boolean project_exprIsNull_1_0) throws java.io.IOException {
//        /* 025 */
//        boolean project_isNull_0 = true;
//        /* 026 */
//        int project_value_0 = -1;
//        /* 027 */
//        boolean project_value_2 = true;
//        /* 028 */
//        /* 029 */
//        if (!project_exprIsNull_0_0) {
//            /* 030 */
//            project_value_2 = project_exprIsNull_1_0;
//            /* 031 */
//        }
//        /* 032 */
//        boolean project_isNull_1 = false;
//        /* 033 */
//        int project_value_1 = -1;
//        /* 034 */
//        if (!false && project_value_2) {
//            /* 035 */
//            project_isNull_1 = true;
//            /* 036 */
//            project_value_1 = -1;
//            /* 037 */
//        } else {
//            /* 038 */
//            Object project_arg_0 = project_exprIsNull_0_0 ? null : ((scala.Function1[]) references[1] /* converters */)[0].apply(project_expr_0_0);
//            /* 039 */
//            Object project_arg_1 = project_exprIsNull_1_0 ? null : ((scala.Function1[]) references[1] /* converters */)[1].apply(project_expr_1_0);
//            /* 040 */
//            /* 041 */
//            Integer project_result_0 = null;
//            /* 042 */
//            try {
//                /* 043 */
//                project_result_0 = (Integer) ((scala.Function1[]) references[1] /* converters */)[2].apply(((scala.Function2) references[3] /* udf */).apply(project_arg_0, project_arg_1));
//                /* 044 */
//            } catch (Exception e) {
//                /* 045 */
//                throw new org.apache.spark.SparkException(((java.lang.String) references[2] /* errMsg */), e);
//                /* 046 */
//            }
//            /* 047 */
//            /* 048 */
//            boolean project_isNull_8 = project_result_0 == null;
//            /* 049 */
//            int project_value_8 = -1;
//            /* 050 */
//            if (!project_isNull_8) {
//                /* 051 */
//                project_value_8 = project_result_0;
//                /* 052 */
//            }
//            /* 053 */
//            project_isNull_1 = project_isNull_8;
//            /* 054 */
//            project_value_1 = project_value_8;
//            /* 055 */
//        }
//        /* 056 */
//        if (!project_isNull_1) {
//            /* 057 */
//            boolean project_value_12 = true;
//            /* 058 */
//            /* 059 */
//            if (!project_exprIsNull_0_0) {
//                /* 060 */
//                project_value_12 = project_exprIsNull_1_0;
//                /* 061 */
//            }
//            /* 062 */
//            boolean project_isNull_11 = false;
//            /* 063 */
//            int project_value_11 = -1;
//            /* 064 */
//            if (!false && project_value_12) {
//                /* 065 */
//                project_isNull_11 = true;
//                /* 066 */
//                project_value_11 = -1;
//                /* 067 */
//            } else {
//                /* 068 */
//                Object project_arg_2 = project_exprIsNull_0_0 ? null : ((scala.Function1[]) references[4] /* converters */)[0].apply(project_expr_0_0);
//                /* 069 */
//                Object project_arg_3 = project_exprIsNull_1_0 ? null : ((scala.Function1[]) references[4] /* converters */)[1].apply(project_expr_1_0);
//                /* 070 */
//                /* 071 */
//                Integer project_result_1 = null;
//                /* 072 */
//                try {
//                    /* 073 */
//                    project_result_1 = (Integer) ((scala.Function1[]) references[4] /* converters */)[2].apply(((scala.Function2) references[6] /* udf */).apply(project_arg_2, project_arg_3));
//                    /* 074 */
//                } catch (Exception e) {
//                    /* 075 */
//                    throw new org.apache.spark.SparkException(((java.lang.String) references[5] /* errMsg */), e);
//                    /* 076 */
//                }
//                /* 077 */
//                /* 078 */
//                boolean project_isNull_18 = project_result_1 == null;
//                /* 079 */
//                int project_value_18 = -1;
//                /* 080 */
//                if (!project_isNull_18) {
//                    /* 081 */
//                    project_value_18 = project_result_1;
//                    /* 082 */
//                }
//                /* 083 */
//                project_isNull_11 = project_isNull_18;
//                /* 084 */
//                project_value_11 = project_value_18;
//                /* 085 */
//            }
//            /* 086 */
//            if (!project_isNull_11) {
//                /* 087 */
//                project_isNull_0 = false; // resultCode could change nullability.
//                /* 088 */
//                project_value_0 = project_value_1 + project_value_11;
//                /* 089 */
//                /* 090 */
//            }
//            /* 091 */
//            /* 092 */
//        }
//        /* 093 */
//        boolean project_value_22 = true;
//        /* 094 */
//        /* 095 */
//        if (!project_exprIsNull_0_0) {
//            /* 096 */
//            project_value_22 = project_exprIsNull_1_0;
//            /* 097 */
//        }
//        /* 098 */
//        boolean project_isNull_21 = false;
//        /* 099 */
//        int project_value_21 = -1;
//        /* 100 */
//        if (!false && project_value_22) {
//            /* 101 */
//            project_isNull_21 = true;
//            /* 102 */
//            project_value_21 = -1;
//            /* 103 */
//        } else {
//            /* 104 */
//            Object project_arg_4 = project_exprIsNull_0_0 ? null : ((scala.Function1[]) references[7] /* converters */)[0].apply(project_expr_0_0);
//            /* 105 */
//            Object project_arg_5 = project_exprIsNull_1_0 ? null : ((scala.Function1[]) references[7] /* converters */)[1].apply(project_expr_1_0);
//            /* 106 */
//            /* 107 */
//            Integer project_result_2 = null;
//            /* 108 */
//            try {
//                /* 109 */
//                project_result_2 = (Integer) ((scala.Function1[]) references[7] /* converters */)[2].apply(((scala.Function2) references[9] /* udf */).apply(project_arg_4, project_arg_5));
//                /* 110 */
//            } catch (Exception e) {
//                /* 111 */
//                throw new org.apache.spark.SparkException(((java.lang.String) references[8] /* errMsg */), e);
//                /* 112 */
//            }
//            /* 113 */
//            /* 114 */
//            boolean project_isNull_28 = project_result_2 == null;
//            /* 115 */
//            int project_value_28 = -1;
//            /* 116 */
//            if (!project_isNull_28) {
//                /* 117 */
//                project_value_28 = project_result_2;
//                /* 118 */
//            }
//            /* 119 */
//            project_isNull_21 = project_isNull_28;
//            /* 120 */
//            project_value_21 = project_value_28;
//            /* 121 */
//        }
//        /* 122 */
//        project_mutableStateArray_0[0].reset();
//        /* 123 */
//        /* 124 */
//        project_mutableStateArray_0[0].zeroOutNullBytes();
//        /* 125 */
//        /* 126 */
//        if (project_isNull_0) {
//            /* 127 */
//            project_mutableStateArray_0[0].setNullAt(0);
//            /* 128 */
//        } else {
//            /* 129 */
//            project_mutableStateArray_0[0].write(0, project_value_0);
//            /* 130 */
//        }
//        /* 131 */
//        /* 132 */
//        if (project_isNull_21) {
//            /* 133 */
//            project_mutableStateArray_0[0].setNullAt(1);
//            /* 134 */
//        } else {
//            /* 135 */
//            project_mutableStateArray_0[0].write(1, project_value_21);
//            /* 136 */
//        }
//        /* 137 */
//        append((project_mutableStateArray_0[0].getRow()));
//        /* 138 */
//        /* 139 */
//    }
//
//    /* 140 */
//    /* 141 */
//    protected void processNext() throws java.io.IOException {
//        /* 142 */
//        while (scan_mutableStateArray_0[0].hasNext()) {
//            /* 143 */
//            InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();
//            /* 144 */
//            ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
//            /* 145 */
//            boolean scan_isNull_0 = scan_row_0.isNullAt(0);
//            /* 146 */
//            int scan_value_0 = scan_isNull_0 ?
//                    /* 147 */       -1 : (scan_row_0.getInt(0));
//            /* 148 */
//            boolean scan_isNull_1 = scan_row_0.isNullAt(1);
//            /* 149 */
//            int scan_value_1 = scan_isNull_1 ?
//                    /* 150 */       -1 : (scan_row_0.getInt(1));
//            /* 151 */
//            /* 152 */
//            project_doConsume_0(scan_row_0, scan_value_0, scan_isNull_0, scan_value_1, scan_isNull_1);
//            /* 153 */
//            if (shouldStop()) return;
//            /* 154 */
//        }
//        /* 155 */
//    }
//    /* 156 */
//    /* 157 */
//}
//
