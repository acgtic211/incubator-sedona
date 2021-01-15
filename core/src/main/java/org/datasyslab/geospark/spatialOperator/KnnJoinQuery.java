/*
 * FILE: JoinQuery
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
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.JoinBuildSide;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.knnJoinJudgement.*;
import org.datasyslab.geospark.monitoring.GeoSparkMetric;
import org.datasyslab.geospark.monitoring.GeoSparkMetrics;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Objects;
import java.util.PriorityQueue;

public class KnnJoinQuery
{
    private static final Logger log = LogManager.getLogger(KnnJoinQuery.class);

    private static <U extends Geometry, T extends Geometry> void verifyCRSMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD)
            throws Exception
    {
        // Check CRS information before doing calculation. The two input RDDs are supposed to have the same EPSG code if they require CRS transformation.
        if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
            throw new IllegalArgumentException("[JoinQuery] input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
        }

        if (spatialRDD.getCRStransformation() && queryRDD.getCRStransformation()) {
            if (!spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode())) {
                throw new IllegalArgumentException("[JoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
            }
        }
    }

    private static <U extends Geometry, T extends Geometry> void verifyPartitioningMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD)
            throws Exception
    {
        Objects.requireNonNull(spatialRDD.spatialPartitionedRDD, "[JoinQuery] spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        Objects.requireNonNull(queryRDD.spatialPartitionedRDD, "[JoinQuery] queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");

        final SpatialPartitioner spatialPartitioner = spatialRDD.getPartitioner();
        final SpatialPartitioner queryPartitioner = queryRDD.getPartitioner();

        if (!queryPartitioner.equals(spatialPartitioner)) {
            throw new IllegalArgumentException("[JoinQuery] queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }

        final int spatialNumPart = spatialRDD.spatialPartitionedRDD.getNumPartitions();
        final int queryNumPart = queryRDD.spatialPartitionedRDD.getNumPartitions();
        if (spatialNumPart != queryNumPart) {
            throw new IllegalArgumentException("[JoinQuery] numbers of partitions in queryRDD and spatialRDD don't match: " + queryNumPart + " vs. " + spatialNumPart + ". Please make sure they both use the same partitioning otherwise wrong results will appear.");
        }
    }

    public static final class JoinParams
    {
        public final boolean useIndex;
        public final boolean considerBoundaryIntersection;
        public final boolean allowDuplicates;
        public final IndexType indexType;
        public final JoinBuildSide joinBuildSide;

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection, boolean allowDuplicates)
        {
            this.useIndex = useIndex;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = allowDuplicates;
            this.indexType = IndexType.RTREE;
            this.joinBuildSide = JoinBuildSide.RIGHT;
        }

        public JoinParams(boolean considerBoundaryIntersection, IndexType polygonIndexType, JoinBuildSide joinBuildSide)
        {
            this.useIndex = false;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = false;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
        }
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, MaxHeap<T>> KnnJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, int k,  boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, true);
        final JavaPairRDD<U, MaxHeap<T>> joinResults = knnJoin(queryRDD, spatialRDD, k, joinParams);
        return joinResults;
    }

    /**
     * <p>
     * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT IT IS.
     * </p>
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, MaxHeap<T>> knnJoin(
            SpatialRDD<U> leftRDD,
            SpatialRDD<T> rightRDD,
            int k,
            JoinParams joinParams)
            throws Exception
    {

        verifyCRSMatch(leftRDD, rightRDD);
        verifyPartitioningMatch(leftRDD, rightRDD);

        SparkContext sparkContext = leftRDD.spatialPartitionedRDD.context();
        GeoSparkMetric buildCount = GeoSparkMetrics.createMetric(sparkContext, "buildCount");
        GeoSparkMetric streamCount = GeoSparkMetrics.createMetric(sparkContext, "streamCount");
        GeoSparkMetric resultCount = GeoSparkMetrics.createMetric(sparkContext, "resultCount");
        GeoSparkMetric candidateCount = GeoSparkMetrics.createMetric(sparkContext, "candidateCount");

        final SpatialPartitioner partitioner =
                (SpatialPartitioner) rightRDD.spatialPartitionedRDD.partitioner().get();
        final DedupParams dedupParams = partitioner.getDedupParams();

        final JavaRDD<Pair<U, KnnData<T>>> resultWithDuplicates;
        JavaRDD<Pair<U, KnnData<T>>> finalKnnLists, nonFinalKnnLists;
        if (joinParams.useIndex) {
            if (rightRDD.indexedRDD != null) {
                final RightIndexLookupJudgement judgement =
                        new RightIndexLookupJudgement(dedupParams, partitioner, k);
                resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement);
                resultWithDuplicates.cache();

                finalKnnLists = resultWithDuplicates.filter(pair -> {
                    return pair.getValue().isFinal;
                });

                nonFinalKnnLists = resultWithDuplicates.filter(pair -> {
                    return !pair.getValue().isFinal;
                });

                JavaRDD<Circle> circles = nonFinalKnnLists.map(pair -> {
                    Circle circle = new Circle(pair.getKey(), pair.getValue().neighbors.getMaxDistance());
                    return circle;
                });

                SpatialRDD<Circle> we = new SpatialRDD<Circle>();
                we.rawSpatialRDD = circles;
                we.spatialPartitioning(partitioner);

                final RangeRightIndexLookupJudgement rangeJudgement =
                        new RangeRightIndexLookupJudgement(dedupParams, k);
                nonFinalKnnLists = we.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, rangeJudgement);

                JavaPairRDD<U, MaxHeap<T>> nonFinalKnnResults = nonFinalKnnLists.mapToPair((PairFunction<Pair<U, KnnData<T>>, U, MaxHeap<T>>) pair -> new Tuple2<U, MaxHeap<T>>(pair.getKey(), pair.getValue().neighbors));

                nonFinalKnnResults = nonFinalKnnResults.reduceByKey((left,right)->{
                    MaxHeap<T> small;
                    MaxHeap<T> large;
                    if(left.getMaxDistance()<right.getMaxDistance()){
                        small = left;
                        large = right;
                    } else {
                        small = right;
                        large = left;
                    }

                    if(large.getMinDistance()>=small.getMaxDistance()){
                        return small;
                    }

                    for(GeometryWithDistance<T> geom:large){
                        small.add(geom);
                    }

                    return small;
                });

                JavaPairRDD<U, MaxHeap<T>> finalKnnResults = finalKnnLists.mapToPair((PairFunction<Pair<U, KnnData<T>>, U, MaxHeap<T>>) pair -> new Tuple2<U, MaxHeap<T>>(pair.getKey(), pair.getValue().neighbors));

                JavaPairRDD<U, MaxHeap<T>> result = finalKnnResults.union(nonFinalKnnResults);
                return result;
            }

        }

        return null;
    }

}

