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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.JoinBuildSide;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.knnJoinJudgement.*;
import org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator;
import org.datasyslab.geospark.monitoring.GeoSparkMetric;
import org.datasyslab.geospark.monitoring.GeoSparkMetrics;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
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
        public final GridType gridType;

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection, boolean allowDuplicates, GridType gridType)
        {
            this.useIndex = useIndex;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = allowDuplicates;
            this.indexType = IndexType.RTREE;
            this.joinBuildSide = JoinBuildSide.RIGHT;
            this.gridType = gridType;
        }

        public JoinParams(boolean considerBoundaryIntersection, IndexType polygonIndexType, JoinBuildSide joinBuildSide)
        {
            this.useIndex = false;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = false;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
            this.gridType = GridType.QUADTREE;
        }
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> KnnJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, int k,  boolean useIndex, boolean considerBoundaryIntersection, GridType gridType)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, true, gridType);
        final JavaPairRDD<U, List<T>> joinResults = knnJoin(queryRDD, spatialRDD, k, joinParams);
        return joinResults;
    }

    /**
     * <p>
     * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT IT IS.
     * </p>
     * @return
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> knnJoin(
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
        JavaRDD<Pair<U, KnnData<T>>> finalKnnLists, nonFinalKnnLists, nonFinalKnnLists2;
        if (joinParams.useIndex) {
            if (rightRDD.indexedRDD != null) {
                final RightIndexLookupJudgement judgement =
                        new RightIndexLookupJudgement(dedupParams, partitioner, k);
                resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement);

            } else {
                DynamicIndexLookupJudgement judgement =
                    new DynamicIndexLookupJudgement(
                        joinParams.considerBoundaryIntersection,
                        joinParams.indexType,
                        joinParams.joinBuildSide,
                        dedupParams,
                        partitioner,
                        k,
                        buildCount, streamCount, resultCount, candidateCount);
                resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
            }
        } else {
            final NestedLoopJudgement judgement =
                    new NestedLoopJudgement(dedupParams, partitioner, k);
            resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
        }

        JavaPairRDD<U, Pair<U,KnnData<T>>> tempResult = resultWithDuplicates.mapToPair((PairFunction<Pair<U, KnnData<T>>, U, Pair<U,KnnData<T>>>) pair -> new Tuple2<U, Pair<U,KnnData<T>>>(pair.getKey(), Pair.of(pair.getKey(), pair.getValue())));
        final JavaRDD<Pair<U, KnnData<T>>> goodResult = tempResult.combineByKey(
                uKnnDataPair -> {
                    PriorityQueue<T> pq = new PriorityQueue<T>(k, new GeometryDistanceComparator(uKnnDataPair.getLeft(), false));
                    for (T curpoint : uKnnDataPair.getRight().neighbors) {
                        pq.offer(curpoint);
                    }
                    return pq;
                },
                ( pq, pair )-> {
                    for(T curpoint: pair.getRight().neighbors){
                        pq.offer(curpoint);
                        if(pq.size()>k){
                            pq.poll();
                        }

                    }
                    return pq;
                },
                ( pq, pq_other )-> {
                    while(!pq_other.isEmpty()){
                        T other = pq_other.poll();
                        if(!pq.contains(other)) {
                            pq.offer(other);
                            if (pq.size() > k) {
                                pq.poll();
                            }
                        }

                    }
                    return pq;
                }).map((Function<Tuple2<U, PriorityQueue<T>>, Pair<U, KnnData<T>>>) pair -> {
            double maxDistance = pair._1.distance(pair._2.peek());
            List<T> neighbors = new ArrayList<>(pair._2);
            boolean isFinal = true;
            final Circle circle = new Circle(pair._1,maxDistance);
            for(Envelope env : partitioner.getGrids()){
                if(circle.getEnvelopeInternal().intersects(env)&&!env.contains(pair._1.getEnvelopeInternal())){
                    isFinal = false;
                    break;
                }
            }
            return Pair.of(pair._1(), new KnnData<T>(neighbors, isFinal, maxDistance));
        });

                    //System.out.println(goodResult.distinct().count());
        final JavaRDD<Pair<U, KnnData<T>>> temp = joinParams.gridType == GridType.VORONOI || joinParams.gridType == GridType.HILBERT ? goodResult : resultWithDuplicates;
        temp.persist(StorageLevel.MEMORY_ONLY());

        finalKnnLists = temp.filter(pair -> pair.getValue().isFinal);

        nonFinalKnnLists = temp.filter(pair -> !pair.getValue().isFinal);

        //System.out.println(finalKnnLists.count());
        //System.out.println(nonFinalKnnLists.count());

        JavaRDD<Circle> circles = nonFinalKnnLists.map(pair -> new Circle(pair.getKey(), pair.getValue().distance));

        SpatialRDD<Circle> spatialCircles = new SpatialRDD<>();
        spatialCircles.rawSpatialRDD = circles;
        spatialCircles.spatialPartitioning(partitioner);

        if (joinParams.useIndex) {
            if (rightRDD.indexedRDD != null) {
                final RangeRightIndexLookupJudgement rangeJudgement =
                        new RangeRightIndexLookupJudgement(dedupParams, partitioner, k);
                nonFinalKnnLists2 = spatialCircles.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, rangeJudgement);

            } else {
                RangeDynamicIndexLookupJudgement rangeJudgement =
                        new RangeDynamicIndexLookupJudgement(
                                joinParams.considerBoundaryIntersection,
                                joinParams.indexType,
                                joinParams.joinBuildSide,
                                dedupParams,
                                partitioner,
                                k,
                                buildCount, streamCount, resultCount, candidateCount);
                nonFinalKnnLists2 = spatialCircles.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, rangeJudgement);
            }
        } else {
            final RangeNestedLoopJudgement rangeJudgement =
                    new RangeNestedLoopJudgement(dedupParams, partitioner, k);
            nonFinalKnnLists2 = spatialCircles.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, rangeJudgement);
        }



        JavaPairRDD<U, Pair<U,KnnData<T>>> nonFinalKnnData = nonFinalKnnLists.union(nonFinalKnnLists2).mapToPair((PairFunction<Pair<U, KnnData<T>>, U, Pair<U,KnnData<T>>>) pair -> new Tuple2<>(pair.getKey(), Pair.of(pair.getKey(),pair.getValue())));

        JavaPairRDD<U, List<T>> nonFinalKnnResults = nonFinalKnnData.combineByKey(
                uKnnDataPair -> {
                    PriorityQueue<T> pq = new PriorityQueue<T>(k, new GeometryDistanceComparator(uKnnDataPair.getLeft(), false));
                    for (T curpoint : uKnnDataPair.getRight().neighbors) {
                        if(curpoint==null)
                            System.out.println(curpoint);
                        pq.offer(curpoint);
                    }
                    return pq;
                },
                ( pq, pair )-> {
                    for(T curpoint: pair.getRight().neighbors){
                        pq.offer(curpoint);
                        if(pq.size()>k){
                            pq.poll();
                        }

                    }
                    return pq;
                },
                ( pq, pq_other )-> {
                    while(!pq_other.isEmpty()){
                        pq.offer(pq_other.poll());
                        if(pq.size()>k){
                            pq.poll();
                        }

                    }
                    return pq;
                }

        ).mapToPair((PairFunction<Tuple2<U, PriorityQueue<T>>, U, List<T>>) pair -> new Tuple2<U, List<T>>(pair._1(), new ArrayList<T>(pair._2)));

        JavaPairRDD<U, List<T>> finalKnnResults = finalKnnLists.mapToPair((PairFunction<Pair<U, KnnData<T>>, U, List<T>>) pair -> new Tuple2<U, List<T>>(pair.getKey(), pair.getValue().neighbors));

        JavaPairRDD<U, List<T>> result = finalKnnResults.union(nonFinalKnnResults);

        return result;

    }

}

