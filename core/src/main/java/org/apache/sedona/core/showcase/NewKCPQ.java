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
package org.apache.sedona.core.showcase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.KCPQAlgorithm;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.core.spatialOperator.NewKCPQuery;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.utils.TimeUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;

// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class NewKCPQ
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

    private static Double sample;

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args)
    {
        String appName = "NEW KCPQ ";
        for(int n = 0; n < args.length; n++){
            appName += args[n];
        }

        SparkConf conf = new SparkConf().setAppName(appName);
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        PointRDD2InputLocation = args[0];
        PointRDDSplitter = FileDataSplitter.CSV;
        PointRDDIndexType = IndexType.RTREE;
        PointRDDOffset = 0;

        PointRDDInputLocation = args[1];

        k = Integer.parseInt(args[2]);

        if(args[3].equals("quadtree")){
            joinQueryPartitioningType = GridType.QUADTREE;
        } else if(args[3].equals("kdbtree")){
            joinQueryPartitioningType = GridType.KDBTREE;
        }
        eachQueryLoopTimes = 1;

        PointRDDIndexType = null;

        partitions = args.length >= 6 ? Integer.parseInt(args[4]) : null;

        sample = args.length == 7 ? Double.parseDouble(args[5]) : 0.001;

        try {
            testKCPQueryUsingIndex();
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
    public static void testKCPQueryUsingIndex()
            throws Exception
    {
        long startTime = System.currentTimeMillis();

        System.out.println("Reading "+PointRDDInputLocation);
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, partitions, StorageLevel.MEMORY_ONLY());
        System.out.println("Reading "+PointRDD2InputLocation);
        PointRDD queryRDD = new PointRDD(sc, PointRDD2InputLocation, PointRDDOffset, PointRDDSplitter, true, partitions);

        System.out.println("Partitioning "+PointRDDInputLocation);
        objectRDD.spatialPartitioning(joinQueryPartitioningType);

        for (int i = 0; i < eachQueryLoopTimes; i++) {
            System.out.println("Joining");

            long resultSize = NewKCPQuery.KClosestPairsQuery(objectRDD, queryRDD, PointRDDIndexType!=null, k, KCPQAlgorithm.REVERSE_FULL, sample).size();

            System.out.println(resultSize);
        }

        System.out.println("elapsed "+TimeUtils.elapsedSince(startTime));

    }

    private static Function2<Integer, Iterator<Point>, Iterator<Integer>> elementsPerPartition() {
        return (integer, pointIterator) -> {
            int n = 0;
            while(pointIterator.hasNext()) {
                n++;
                pointIterator.next();
            }
            return Collections.singletonList(n).iterator();
        };
    }

}