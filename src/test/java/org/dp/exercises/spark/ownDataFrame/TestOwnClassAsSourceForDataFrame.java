package org.dp.exercises.spark.ownDataFrame;

import com.google.common.collect.Iterables;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratedExpressionCode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;
import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class TestOwnClassAsSourceForDataFrame {


    @Test
    public void shouldUsePojoClassAsDataFrame(){
        SparkConf conf = new SparkConf().setAppName("test1")
                .setMaster("local")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<Dog> collection = sc.parallelize(Arrays.asList(new Dog("Hyde", 3), new Dog("Leon", 6)));
        DataFrame dataFrame = sqlContext.createDataFrame(collection, Dog.class);
//        JavaRDD<String> collectionString = sc.parallelize(Arrays.asList("warszawa", "wroclaw"));

        System.out.println(dataFrame.count());
        dataFrame.show();
    }

    @Test
    public void shouldCreateDataFrameOnHashMap(){

        Entity dog1 = new Entity();
        dog1.addAttribute("name", "Hyde");
        dog1.addAttribute("age", 3);

        Entity dog2 = new Entity();
        dog2.addAttribute("name", "Leon");
        dog2.addAttribute("age", 6);

        SparkConf conf = new SparkConf().setAppName("test1")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<Entity> collection = sc.parallelize(Arrays.asList(dog1, dog2));

        Seq<String> nil = JavaConversions.asScalaBuffer(new ArrayList<>());
        AttributeReference nameAttributeReference = new AttributeReference("name", StringType$.MODULE$, true, Metadata.empty(), NamedExpression$.MODULE$.newExprId(), nil);
        AttributeReference ageAttributeReference = new AttributeReference("age", IntegerType$.MODULE$, true, Metadata.empty(), NamedExpression$.MODULE$.newExprId(), nil);

        Seq<Attribute> attributes = JavaConversions.asScalaBuffer(Arrays.asList(nameAttributeReference, ageAttributeReference));
//        collection.mapPartitions()

        JavaRDD<InternalRow> rdd = collection.mapPartitions(new LazyMapper(),
                false);
        DataFrame dataFrame = new DataFrame(sqlContext, new LogicalRDD(attributes, rdd.rdd(), sqlContext));
        dataFrame.show();
//        DataFrame filteredDataFrame = dataFrame.filter("age > 3");
//        filteredDataFrame.show();

        DataFrame persistedOnDIsk = dataFrame.persist(StorageLevel.DISK_ONLY());
        persistedOnDIsk.withColumn("new_col", org.apache.spark.sql.functions.lit("z"));
        persistedOnDIsk.show();
        JavaRDD<Object> rdd1 = persistedOnDIsk.javaRDD()
                .map(
                        (Function<Row, Object>) v1 -> v1
                );
        System.out.println(rdd1.count());
//        persistedOnDIsk.show();
    }

    @Test
    public void shouldUseUserDefinedType(){

        Entity dog1 = new Entity();
        dog1.addAttribute("name", "Hyde");
        dog1.addAttribute("age", 3);
        dog1.addAttribute("someId", new RefDataId(1));

        Entity dog2 = new Entity();
        dog2.addAttribute("name", "Leon");
        dog2.addAttribute("age", 6);
        dog1.addAttribute("someId", new RefDataId(2));

        SparkConf conf = new SparkConf().setAppName("test1")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<Entity> collection = sc.parallelize(Arrays.asList(dog1, dog2));

        Seq<String> nil = JavaConversions.asScalaBuffer(new ArrayList<>());
        AttributeReference nameAttributeReference = new AttributeReference("name", StringType$.MODULE$, true, Metadata.empty(), NamedExpression$.MODULE$.newExprId(), nil);
        AttributeReference ageAttributeReference = new AttributeReference("age", IntegerType$.MODULE$, true, Metadata.empty(), NamedExpression$.MODULE$.newExprId(), nil);
    }

    @Test
    public void shouldAddColumn(){

        Entity dog1 = new Entity();
        dog1.addAttribute("name", "Hyde");
        dog1.addAttribute("age", 3);

        Entity dog2 = new Entity();
        dog2.addAttribute("name", "Leon");
        dog2.addAttribute("age", 6);

        SparkConf conf = new SparkConf().setAppName("test1")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<Entity> collection = sc.parallelize(Arrays.asList(dog1, dog2));

        Seq<String> nil = JavaConversions.asScalaBuffer(new ArrayList<>());
        AttributeReference nameAttributeReference = new AttributeReference("name", StringType$.MODULE$, true, Metadata.empty(), NamedExpression$.MODULE$.newExprId(), nil);
        AttributeReference ageAttributeReference = new AttributeReference("age", IntegerType$.MODULE$, true, Metadata.empty(), NamedExpression$.MODULE$.newExprId(), nil);

        Seq<Attribute> attributes = JavaConversions.asScalaBuffer(Arrays.asList(nameAttributeReference, ageAttributeReference));
//        collection.mapPartitions()

        JavaRDD<InternalRow> rdd = collection.mapPartitions(new LazyMapper(), false);
        DataFrame dataFrame = new DataFrame(sqlContext, new LogicalRDD(attributes, rdd.rdd(), sqlContext));
        dataFrame.show();

        DataFrame dataFrame1 = dataFrame.withColumn("new_col", functions.lit("z"));
        dataFrame1.show();
    }

    public static class LazyMapper implements FlatMapFunction<Iterator<Entity>, InternalRow>{

        @Override
        public Iterable<InternalRow> call(Iterator<Entity> entityIterator) throws Exception {
            Dog dog = new Dog("x", 22);
            return new Iterable<InternalRow>(){

                @Override
                public Iterator<InternalRow> iterator() {
                    return new Iterator<InternalRow>() {
                        @Override
                        public boolean hasNext() {
                            return entityIterator.hasNext();
                        }

                        @Override
                        public InternalRow next() {
                            Entity next = entityIterator.next();
                            return new EntityInternalRow(next, dog);
                        }
                    };
                }
            };
        }
    }

    public static class EntityInternalRow extends MutableRow{

        private Entity entity;
        private Dog dog;

        public EntityInternalRow(Entity entity, Dog dog) {
            this.entity = entity;
            this.dog = dog;
        }

        private String intToString(int i){
            switch (i){
                case 0: return "name";
                case 1: return "age";
                default:
                    throw new RuntimeException("Incorrect index");
            }

        }

        @Override
        public void setNullAt(int i) {

        }

        @Override
        public void update(int i, Object value) {

        }

        @Override
        public int numFields() {
            return 2;
        }

        @Override
        public InternalRow copy() {
            Entity target = new Entity();

            entity.attributes.entrySet().forEach( e -> target.addAttribute(e.getKey(), e.getValue()));
            return new EntityInternalRow(target, dog);
        }

        @Override
        public boolean anyNull() {
            return false;
        }

        @Override
        public boolean isNullAt(int ordinal) {
            String colName = intToString(ordinal);
            return entity.getAttirubute(colName)==null;
        }

        @Override
        public boolean getBoolean(int ordinal) {
            return false;
        }

        @Override
        public byte getByte(int ordinal) {
            return 0;
        }

        @Override
        public short getShort(int ordinal) {
            return 0;
        }

        @Override
        public int getInt(int ordinal) {
            String attrName = intToString(ordinal);
            return (int) entity.getAttirubute(attrName);
        }

        @Override
        public long getLong(int ordinal) {
            return 0;
        }

        @Override
        public float getFloat(int ordinal) {
            return 0;
        }

        @Override
        public double getDouble(int ordinal) {
            return 0;
        }

        @Override
        public Decimal getDecimal(int ordinal, int precision, int scale) {
            return null;
        }

        @Override
        public UTF8String getUTF8String(int ordinal) {
            String attrName = intToString(ordinal);
            return UTF8String.fromString(entity.getAttirubute(attrName).toString());
        }

        @Override
        public byte[] getBinary(int ordinal) {
            return new byte[0];
        }

        @Override
        public CalendarInterval getInterval(int ordinal) {
            return null;
        }

        @Override
        public InternalRow getStruct(int ordinal, int numFields) {
            return null;
        }

        @Override
        public ArrayData getArray(int ordinal) {
            return null;
        }

        @Override
        public MapData getMap(int ordinal) {
            return null;
        }

        @Override
        public Object get(int ordinal, DataType dataType) {
            return null;
        }
    }


    public void shouldUseOwnClassAsDataFrame(){

    }
}
