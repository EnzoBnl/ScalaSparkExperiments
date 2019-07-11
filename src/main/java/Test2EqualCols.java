import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

public class Test2EqualCols {
    // Remove line numbers comments: /\* \d{3} \*/
//    Found 1 WholeStageCodegen subtrees.
//            == Subtree 1 / 1 ==
//            *(1) Project [(hash(cast((age#11 + education_num#15) as string), 42) + hash(cast((age#11 + education_num#15) as string), 42)) AS (hash(CAST((age + education_num) AS STRING)) + hash(CAST((age + education_num) AS STRING)))#46, hash(cast((age#11 + education_num#15) as string), 42) AS hash(CAST((age + education_num) AS STRING))#47, hash(cast((age#11 + education_num#15) as string), 42) AS hash(CAST((age + education_num) AS STRING))#48]
//            +- *(1) FileScan csv [age#11,education_num#15] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Applications/khiops/samples/Adult/Adult.txt], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<age:int,education_num:int>
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
                 project_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 0);
            
               }
        
           private void project_doConsume_0(InternalRow scan_row_0, int project_expr_0_0, boolean project_exprIsNull_0_0, int project_expr_1_0, boolean project_exprIsNull_1_0) throws java.io.IOException {
                 int project_value_1 = 42;
                 boolean project_isNull_3 = true;
                 int project_value_3 = -1;
            
                 if (!project_exprIsNull_0_0) {
                       if (!project_exprIsNull_1_0) {
                             project_isNull_3 = false; // resultCode could change nullability.
                             project_value_3 = project_expr_0_0 + project_expr_1_0;
                    
                           }
                
                     }
                 boolean project_isNull_2 = project_isNull_3;
                 UTF8String project_value_2 = null;
                 if (!project_isNull_3) {
                       project_value_2 = UTF8String.fromString(String.valueOf(project_value_3));
                     }
                 if (!project_isNull_2) {
                       project_value_1 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(project_value_2.getBaseObject(), project_value_2.getBaseOffset(), project_value_2.numBytes(), project_value_1);
                     }
                 int project_value_6 = 42;
                 boolean project_isNull_8 = true;
                 int project_value_8 = -1;
            
                 if (!project_exprIsNull_0_0) {
                       if (!project_exprIsNull_1_0) {
                             project_isNull_8 = false; // resultCode could change nullability.
                             project_value_8 = project_expr_0_0 + project_expr_1_0;
                    
                           }
                
                     }
                 boolean project_isNull_7 = project_isNull_8;
                 UTF8String project_value_7 = null;
                 if (!project_isNull_8) {
                       project_value_7 = UTF8String.fromString(String.valueOf(project_value_8));
                     }
                 if (!project_isNull_7) {
                       project_value_6 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(project_value_7.getBaseObject(), project_value_7.getBaseOffset(), project_value_7.numBytes(), project_value_6);
                     }
                 int project_value_0 = -1;
                 project_value_0 = project_value_1 + project_value_6;
                 int project_value_11 = 42;
                 boolean project_isNull_13 = true;
                 int project_value_13 = -1;
            
                 if (!project_exprIsNull_0_0) {
                       if (!project_exprIsNull_1_0) {
                             project_isNull_13 = false; // resultCode could change nullability.
                             project_value_13 = project_expr_0_0 + project_expr_1_0;
                    
                           }
                
                     }
                 boolean project_isNull_12 = project_isNull_13;
                 UTF8String project_value_12 = null;
                 if (!project_isNull_13) {
                       project_value_12 = UTF8String.fromString(String.valueOf(project_value_13));
                     }
                 if (!project_isNull_12) {
                       project_value_11 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(project_value_12.getBaseObject(), project_value_12.getBaseOffset(), project_value_12.numBytes(), project_value_11);
                     }
                 int project_value_16 = 42;
                 boolean project_isNull_18 = true;
                 int project_value_18 = -1;
            
                 if (!project_exprIsNull_0_0) {
                       if (!project_exprIsNull_1_0) {
                             project_isNull_18 = false; // resultCode could change nullability.
                             project_value_18 = project_expr_0_0 + project_expr_1_0;
                    
                           }
                
                     }
                 boolean project_isNull_17 = project_isNull_18;
                 UTF8String project_value_17 = null;
                 if (!project_isNull_18) {
                       project_value_17 = UTF8String.fromString(String.valueOf(project_value_18));
                     }
                 if (!project_isNull_17) {
                       project_value_16 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes(project_value_17.getBaseObject(), project_value_17.getBaseOffset(), project_value_17.numBytes(), project_value_16);
                     }
                 project_mutableStateArray_0[0].reset();
            
                 project_mutableStateArray_0[0].write(0, project_value_0);
            
                 project_mutableStateArray_0[0].write(1, project_value_11);
            
                 project_mutableStateArray_0[0].write(2, project_value_16);
                 append((project_mutableStateArray_0[0].getRow()));
            
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
                
                       project_doConsume_0(scan_row_0, scan_value_0, scan_isNull_0, scan_value_1, scan_isNull_1);
                       if (shouldStop()) return;
                     }
               }
        
         }
}