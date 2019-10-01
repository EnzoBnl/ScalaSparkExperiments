import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

//Found 1 WholeStageCodegen subtrees.
//        == Subtree 1 / 1 ==
//        *(1) SerializeFromObject [assertnotnull(input[0, scala.Tuple4, true])._1 AS _1#75, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple4, true])._2, true, false) AS _2#76, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple4, true])._3, true, false) AS _3#77, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple4, true])._4, true, false) AS _4#78]
//        +- *(1) Filter <function1>.apply
//        +- *(1) MapElements <function1>, obj#74: scala.Tuple4
//        +- *(1) DeserializeToObject newInstance(class com.enzobnl.sparkscalaexpe.playground.User), obj#73: com.enzobnl.sparkscalaexpe.playground.User
//        +- *(1) Project [_c0#53 AS id#59, _c1#54 AS pseudo#60, _c2#55 AS name#61]
//        +- *(1) FileScan csv [_c0#53,_c1#54,_c2#55] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Prog/spark/data/graphx/users.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<_c0:int,_c1:string,_c2:string>
//
//        Generated code:

/**
 * Dataset new col with substring and filter on it
 * val ds = spark.read
 *       .format("csv")
 *       .option("header", "false")
 *       .option("delimiter", ",")
 *       .schema(StructType(Seq(
 *         StructField("id", IntegerType, true),
 *         StructField("pseudo", StringType, true),
 *         StructField("name", StringType, true))))
 *       .load("/home/enzo/Data/sofia-air-quality-dataset/2019-05_bme280sof.csv")
 *       .toDF("id", "pseudo", "name")
 *       .as[User]
 *       .map((user: User) => if(user.name != null)(user.id, user.name, user.pseudo, user.name.substring(1)) else (user.id, user.name, user.pseudo, ""))
 *       .filter((extendedUser: (Int, String, String, String)) => extendedUser._4.startsWith("a"))
 */
public class DFvsDS {
    public Object generate(Object[] references) {
        return new GeneratedIteratorForCodegenStage1(references);
    }

    // codegenStageId=1
    final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
        private Object[] references;
        private scala.collection.Iterator[] inputs;
        private int deserializetoobject_argValue_0;
        private boolean mapelements_resultIsNull_0;
        private boolean filter_resultIsNull_0;
        private boolean serializefromobject_resultIsNull_0;
        private boolean serializefromobject_resultIsNull_1;
        private boolean serializefromobject_resultIsNull_2;
        private java.lang.String[] deserializetoobject_mutableStateArray_0 = new java.lang.String[5];
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] project_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[8];
        private scala.collection.Iterator[] scan_mutableStateArray_0 = new scala.collection.Iterator[1];
        private scala.Tuple4[] filter_mutableStateArray_0 = new scala.Tuple4[1];
        private com.enzobnl.sparkscalaexpe.playground.User[] mapelements_mutableStateArray_0 = new com.enzobnl.sparkscalaexpe.playground.User[1];

        public GeneratedIteratorForCodegenStage1(Object[] references) {
            this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
            partitionIndex = index;
            this.inputs = inputs;
            scan_mutableStateArray_0[0] = inputs[0];
            project_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 64);
            project_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 64);

            project_mutableStateArray_0[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
            project_mutableStateArray_0[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
            project_mutableStateArray_0[4] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
            project_mutableStateArray_0[5] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
            project_mutableStateArray_0[6] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
            // only this one is used to write final Tuple4 rows with append((project_mutableStateArray_0[7].getRow()));
            // append is inherited from BufferedRowIterator.
            project_mutableStateArray_0[7] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);

        }
        private void mapelements_doConsume_0(com.enzobnl.sparkscalaexpe.playground.User mapelements_expr_0_0, boolean mapelements_exprIsNull_0_0) throws java.io.IOException {
            do {
                boolean mapelements_isNull_1 = true;
                scala.Tuple4 mapelements_value_1 = null;
                if (!false) {
                    mapelements_resultIsNull_0 = false;

                    if (!mapelements_resultIsNull_0) {
                        mapelements_resultIsNull_0 = mapelements_exprIsNull_0_0;
                        mapelements_mutableStateArray_0[0] = mapelements_expr_0_0;
                    }

                    mapelements_isNull_1 = mapelements_resultIsNull_0;
                    if (!mapelements_isNull_1) {
                        Object mapelements_funcResult_0 = null;
                        // apply map anonymous function stored in (scala.Function1) references[2]
                        // on custom case class User stored in mapelements_mutableStateArray_0[0]
                        mapelements_funcResult_0 = ((scala.Function1) references[2]).apply(mapelements_mutableStateArray_0[0]);

                        if (mapelements_funcResult_0 != null) {
                            mapelements_value_1 = (scala.Tuple4) mapelements_funcResult_0;
                        } else {
                            mapelements_isNull_1 = true;
                        }

                    }
                }

                boolean filter_isNull_0 = true;
                boolean filter_value_0 = false;
                if (!false) {
                    filter_resultIsNull_0 = false;

                    if (!filter_resultIsNull_0) {
                        filter_resultIsNull_0 = mapelements_isNull_1;
                        filter_mutableStateArray_0[0] = mapelements_value_1;
                    }

                    filter_isNull_0 = filter_resultIsNull_0;
                    if (!filter_isNull_0) {
                        Object filter_funcResult_0 = null;
                        filter_funcResult_0 = ((scala.Function1) references[4]).apply(filter_mutableStateArray_0[0]);

                        if (filter_funcResult_0 != null) {
                            filter_value_0 = (Boolean) filter_funcResult_0;
                        } else {
                            filter_isNull_0 = true;
                        }

                    }
                }
                // if filter failed, skip line by not calling
                // serializefromobject_doConsume_0(scala.Tuple4 mapelements_value_1, boolean mapelements_isNull_1)
                if (filter_isNull_0 || !filter_value_0) continue;

                ((org.apache.spark.sql.execution.metric.SQLMetric) references[3]).add(1);

                serializefromobject_doConsume_0(mapelements_value_1, mapelements_isNull_1);

            } while (false);

        }

        private void deserializetoobject_doConsume_0(int deserializetoobject_expr_0_0, boolean deserializetoobject_exprIsNull_0_0, UTF8String deserializetoobject_expr_1_0, boolean deserializetoobject_exprIsNull_1_0, UTF8String deserializetoobject_expr_2_0, boolean deserializetoobject_exprIsNull_2_0) throws java.io.IOException {
            if (deserializetoobject_exprIsNull_0_0) {
                throw new NullPointerException(((java.lang.String) references[1]));
            }
            deserializetoobject_argValue_0 = deserializetoobject_expr_0_0;

            boolean deserializetoobject_isNull_6 = true;
            java.lang.String deserializetoobject_value_6 = null;
            if (!deserializetoobject_exprIsNull_1_0) {
                deserializetoobject_isNull_6 = false;
                if (!deserializetoobject_isNull_6) {
                    Object deserializetoobject_funcResult_0 = null;
                    deserializetoobject_funcResult_0 = deserializetoobject_expr_1_0.toString();
                    deserializetoobject_value_6 = (java.lang.String) deserializetoobject_funcResult_0;

                }
            }
            deserializetoobject_mutableStateArray_0[0] = deserializetoobject_value_6;

            boolean deserializetoobject_isNull_8 = true;
            java.lang.String deserializetoobject_value_8 = null;
            if (!deserializetoobject_exprIsNull_2_0) {
                deserializetoobject_isNull_8 = false;
                if (!deserializetoobject_isNull_8) {
                    Object deserializetoobject_funcResult_1 = null;
                    deserializetoobject_funcResult_1 = deserializetoobject_expr_2_0.toString();
                    deserializetoobject_value_8 = (java.lang.String) deserializetoobject_funcResult_1;

                }
            }
            deserializetoobject_mutableStateArray_0[1] = deserializetoobject_value_8;

            final com.enzobnl.sparkscalaexpe.playground.User deserializetoobject_value_3 = false ?
                    null : new com.enzobnl.sparkscalaexpe.playground.User(deserializetoobject_argValue_0, deserializetoobject_mutableStateArray_0[0], deserializetoobject_mutableStateArray_0[1]);

            mapelements_doConsume_0(deserializetoobject_value_3, false);

        }

        private void serializefromobject_doConsume_0(scala.Tuple4 serializefromobject_expr_0_0, boolean serializefromobject_exprIsNull_0_0) throws java.io.IOException {
            if (serializefromobject_exprIsNull_0_0) {
                throw new NullPointerException(((java.lang.String) references[5]));
            }
            boolean serializefromobject_isNull_1 = true;
            int serializefromobject_value_1 = -1;
            if (!false) {
                serializefromobject_isNull_1 = false;
                if (!serializefromobject_isNull_1) {
                    Object serializefromobject_funcResult_0 = null;
                    serializefromobject_funcResult_0 = serializefromobject_expr_0_0._1();
                    serializefromobject_value_1 = (Integer) serializefromobject_funcResult_0;

                }
            }
            serializefromobject_resultIsNull_0 = false;

            if (!serializefromobject_resultIsNull_0) {
                if (serializefromobject_exprIsNull_0_0) {
                    throw new NullPointerException(((java.lang.String) references[6]));
                }
                boolean serializefromobject_isNull_5 = true;
                java.lang.String serializefromobject_value_5 = null;
                if (!false) {
                    serializefromobject_isNull_5 = false;
                    if (!serializefromobject_isNull_5) {
                        Object serializefromobject_funcResult_1 = null;
                        serializefromobject_funcResult_1 = serializefromobject_expr_0_0._2();

                        if (serializefromobject_funcResult_1 != null) {
                            serializefromobject_value_5 = (java.lang.String) serializefromobject_funcResult_1;
                        } else {
                            serializefromobject_isNull_5 = true;
                        }

                    }
                }
                serializefromobject_resultIsNull_0 = serializefromobject_isNull_5;
                deserializetoobject_mutableStateArray_0[2] = serializefromobject_value_5;
            }

            boolean serializefromobject_isNull_4 = serializefromobject_resultIsNull_0;
            UTF8String serializefromobject_value_4 = null;
            if (!serializefromobject_resultIsNull_0) {
                serializefromobject_value_4 = org.apache.spark.unsafe.types.UTF8String.fromString(deserializetoobject_mutableStateArray_0[2]);
            }
            serializefromobject_resultIsNull_1 = false;

            if (!serializefromobject_resultIsNull_1) {
                if (serializefromobject_exprIsNull_0_0) {
                    throw new NullPointerException(((java.lang.String) references[7]));
                }
                boolean serializefromobject_isNull_9 = true;
                java.lang.String serializefromobject_value_9 = null;
                if (!false) {
                    serializefromobject_isNull_9 = false;
                    if (!serializefromobject_isNull_9) {
                        Object serializefromobject_funcResult_2 = null;
                        serializefromobject_funcResult_2 = serializefromobject_expr_0_0._3();

                        if (serializefromobject_funcResult_2 != null) {
                            serializefromobject_value_9 = (java.lang.String) serializefromobject_funcResult_2;
                        } else {
                            serializefromobject_isNull_9 = true;
                        }

                    }
                }
                serializefromobject_resultIsNull_1 = serializefromobject_isNull_9;
                deserializetoobject_mutableStateArray_0[3] = serializefromobject_value_9;
            }

            boolean serializefromobject_isNull_8 = serializefromobject_resultIsNull_1;
            UTF8String serializefromobject_value_8 = null;
            if (!serializefromobject_resultIsNull_1) {
                serializefromobject_value_8 = org.apache.spark.unsafe.types.UTF8String.fromString(deserializetoobject_mutableStateArray_0[3]);
            }
            serializefromobject_resultIsNull_2 = false;

            if (!serializefromobject_resultIsNull_2) {
                if (serializefromobject_exprIsNull_0_0) {
                    throw new NullPointerException(((java.lang.String) references[8]));
                }
                boolean serializefromobject_isNull_13 = true;
                java.lang.String serializefromobject_value_13 = null;
                if (!false) {
                    serializefromobject_isNull_13 = false;
                    if (!serializefromobject_isNull_13) {
                        Object serializefromobject_funcResult_3 = null;
                        serializefromobject_funcResult_3 = serializefromobject_expr_0_0._4();

                        if (serializefromobject_funcResult_3 != null) {
                            serializefromobject_value_13 = (java.lang.String) serializefromobject_funcResult_3;
                        } else {
                            serializefromobject_isNull_13 = true;
                        }
                    }
                }
                serializefromobject_resultIsNull_2 = serializefromobject_isNull_13;
                deserializetoobject_mutableStateArray_0[4] = serializefromobject_value_13;
            }

            boolean serializefromobject_isNull_12 = serializefromobject_resultIsNull_2;
            UTF8String serializefromobject_value_12 = null;
            if (!serializefromobject_resultIsNull_2) {
                serializefromobject_value_12 = org.apache.spark.unsafe.types.UTF8String.fromString(deserializetoobject_mutableStateArray_0[4]);
            }
            project_mutableStateArray_0[7].reset();

            project_mutableStateArray_0[7].zeroOutNullBytes();

            project_mutableStateArray_0[7].write(0, serializefromobject_value_1);

            if (serializefromobject_isNull_4) {
                project_mutableStateArray_0[7].setNullAt(1);
            } else {
                project_mutableStateArray_0[7].write(1, serializefromobject_value_4);
            }

            if (serializefromobject_isNull_8) {
                project_mutableStateArray_0[7].setNullAt(2);
            } else {
                project_mutableStateArray_0[7].write(2, serializefromobject_value_8);
            }

            if (serializefromobject_isNull_12) {
                project_mutableStateArray_0[7].setNullAt(3);
            } else {
                project_mutableStateArray_0[7].write(3, serializefromobject_value_12);
            }
            append((project_mutableStateArray_0[7].getRow()));

        }

        protected void processNext() throws java.io.IOException {
            while (scan_mutableStateArray_0[0].hasNext()) {
                InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[0]).add(1);
                boolean scan_isNull_0 = scan_row_0.isNullAt(0);
                int scan_value_0 = scan_isNull_0 ?
                        -1 : (scan_row_0.getInt(0));
                boolean scan_isNull_1 = scan_row_0.isNullAt(1);
                UTF8String scan_value_1 = scan_isNull_1 ?
                        null : (scan_row_0.getUTF8String(1));
                boolean scan_isNull_2 = scan_row_0.isNullAt(2);
                UTF8String scan_value_2 = scan_isNull_2 ?
                        null : (scan_row_0.getUTF8String(2));

                deserializetoobject_doConsume_0(scan_value_0, scan_isNull_0, scan_value_1, scan_isNull_1, scan_value_2, scan_isNull_2);
                if (shouldStop()) return;
            }
        }

    }
}

/**
 * DataFrame new col with substring and filter on it
 * val df = spark.read
 *       .format("csv")
 *       .option("header", "false")
 *       .option("delimiter", ",")
 *       .schema(StructType(Seq(
 *         StructField("id", IntegerType, true),
 *         StructField("pseudo", StringType, true),
 *         StructField("name", StringType, true))))
 *       .load("/home/enzo/Data/sofia-air-quality-dataset/2019-05_bme280sof.csv")
 *       .toDF("id", "pseudo", "name")
 *       .selectExpr("*", "substr(pseudo, 2) AS sub")
 *       .filter("sub LIKE 'a%' ")
 */
class DFvsDS2 {

    //Found 1 WholeStageCodegen subtrees.
//        == Subtree 1 / 1 ==
//        *(1) Project [_c0#10 AS id#16, _c1#11 AS pseudo#17, _c2#12 AS name#18, substring(_c1#11, 2, 2147483647) AS sub#22]
//        +- *(1) Filter (isnotnull(_c1#11) && StartsWith(substring(_c1#11, 2, 2147483647), a))
//        +- *(1) FileScan csv [_c0#10,_c1#11,_c2#12] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/enzo/Prog/spark/data/graphx/users.txt], PartitionFilters: [], PushedFilters: [IsNotNull(_c1)], ReadSchema: struct<_c0:int,_c1:string,_c2:string>
//
//        Generated code:
    public Object generate(Object[] references) {
        return new GeneratedIteratorForCodegenStage1(references);
    }

    // codegenStageId=1
    final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
        private Object[] references;
        private scala.collection.Iterator[] inputs;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] filter_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];
        private scala.collection.Iterator[] scan_mutableStateArray_0 = new scala.collection.Iterator[1];

        public GeneratedIteratorForCodegenStage1(Object[] references) {
            this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
            // inputs are Iterators of InternalRows: there is no data contiguousity inter rows -> not CPU caches optimized
            partitionIndex = index;
            this.inputs = inputs;
            scan_mutableStateArray_0[0] = inputs[0];
            filter_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 64);
            filter_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 96);
        }

        protected void processNext() throws java.io.IOException {
            // scan_mutableStateArray_0[0] filled with inputs iterator in init
            while (scan_mutableStateArray_0[0].hasNext()) {
                // get next input InternalRow
                InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[0]).add(1);
                do {
                    boolean scan_isNull_1 = scan_row_0.isNullAt(1);
                    // getUTF8String: direct access to the string without deserialization
                    UTF8String scan_value_1 = scan_isNull_1 ?
                            null : (scan_row_0.getUTF8String(1));

                    if (!(!scan_isNull_1)) continue;

                    UTF8String filter_value_3 = null;
                    filter_value_3 = scan_value_1.substringSQL(2, 2147483647);

                    boolean filter_value_2 = false;
                    filter_value_2 = (filter_value_3).startsWith(((UTF8String) references[2]));
                    // if not startwith: skip and continue to next record
                    if (!filter_value_2) continue;

                    ((org.apache.spark.sql.execution.metric.SQLMetric) references[1]).add(1);

                    boolean scan_isNull_0 = scan_row_0.isNullAt(0);
                    int scan_value_0 = scan_isNull_0 ?
                            -1 : (scan_row_0.getInt(0));
                    boolean scan_isNull_2 = scan_row_0.isNullAt(2);
                    UTF8String scan_value_2 = scan_isNull_2 ?
                            null : (scan_row_0.getUTF8String(2));
                    UTF8String project_value_3 = null;
                    project_value_3 = scan_value_1.substringSQL(2, 2147483647);
                    filter_mutableStateArray_0[1].reset();

                    filter_mutableStateArray_0[1].zeroOutNullBytes();

                    if (scan_isNull_0) {
                        filter_mutableStateArray_0[1].setNullAt(0);
                    } else {
                        filter_mutableStateArray_0[1].write(0, scan_value_0);
                    }

                    if (false) {
                        filter_mutableStateArray_0[1].setNullAt(1);
                    } else {
                        filter_mutableStateArray_0[1].write(1, scan_value_1);
                    }

                    if (scan_isNull_2) {
                        filter_mutableStateArray_0[1].setNullAt(2);
                    } else {
                        filter_mutableStateArray_0[1].write(2, scan_value_2);
                    }

                    if (false) {
                        filter_mutableStateArray_0[1].setNullAt(3);
                    } else {
                        filter_mutableStateArray_0[1].write(3, project_value_3);
                    }
                    append((filter_mutableStateArray_0[1].getRow()));

                } while (false);
                if (shouldStop()) return;
            }
        }

    }
}