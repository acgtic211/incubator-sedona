/*
 * FILE: Example
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.showcase;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.KnnJoinQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import scala.Int;

import java.io.Serializable;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class KNNJ
        implements Serializable
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * The geometry factory.
     */
    static GeometryFactory geometryFactory;

    /**
     * The Point RDD input location.
     */
    static String PointRDDInputLocation;

    /**
     * The Point RDD input location.
     */
    static String PointRDD2InputLocation;

    /**
     * The Point RDD offset.
     */
    static Integer PointRDDOffset;

    /**
     * The Point RDD splitter.
     */
    static FileDataSplitter PointRDDSplitter;

    /**
     * The Point RDD index type.
     */
    static IndexType PointRDDIndexType;

    /**
     * The object RDD.
     */
    static PointRDD objectRDD;

    /**
     * The join query partitioning type.
     */
    static GridType joinQueryPartitioningType;



    /**
     * The each query loop times.
     */
    static int eachQueryLoopTimes;

    /**
     * The number of neighbours.
     */
    
    private static int k;

    private static Integer partitions;

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        PointRDD2InputLocation = args[0];
        PointRDDSplitter = FileDataSplitter.CSV;
        PointRDDIndexType = IndexType.RTREE;
        PointRDDOffset = 0;

        PointRDDInputLocation = args[1];

        k = Integer.parseInt(args[2]);
        joinQueryPartitioningType = GridType.QUADTREE;
        eachQueryLoopTimes = 1;

        partitions = args.length == 4 ? Integer.parseInt(args[3]) : 500;

        try {
            testKnnJoinQueryUsingIndex();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("GeoSpark DEMOs failed!");
            return;
        }
        sc.stop();
        System.out.println("All GeoSpark DEMOs passed!");
    }

    /**
     * Test knn join query using index.
     *
     * @throws Exception the exception
     */
    public static void testKnnJoinQueryUsingIndex()
            throws Exception
    {
        System.out.println("Reading "+PointRDDInputLocation);
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, partitions, StorageLevel.MEMORY_ONLY());
        //objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        System.out.println("Reading "+PointRDD2InputLocation);
        PointRDD queryRDD = new PointRDD(sc, PointRDD2InputLocation, PointRDDOffset, PointRDDSplitter, true, partitions, StorageLevel.MEMORY_ONLY());
        //PointRDD queryRDD = new PointRDD(sc, PointRDD2InputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());

        System.out.println("Partitioning "+PointRDDInputLocation);
        objectRDD.spatialPartitioning(joinQueryPartitioningType);
        System.out.println("Partitioning "+PointRDD2InputLocation);
        queryRDD.spatialPartitioning(objectRDD.getPartitioner());

        System.out.println("Build Index "+PointRDDInputLocation);
        objectRDD.buildIndex(PointRDDIndexType, true);
        objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        for (int i = 0; i < eachQueryLoopTimes; i++) {
            System.out.println("Joining");

            long resultSize = KnnJoinQuery.KnnJoinQuery(objectRDD, queryRDD, k,true, true).count();
            assert resultSize > 0;
        }
    }

}