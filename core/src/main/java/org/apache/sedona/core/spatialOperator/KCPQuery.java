/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.spatialOperator;

import org.apache.hadoop.util.PriorityQueue;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.enums.KCPQAlgorithm;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.kcpJudgement.DistanceAndPair;
import org.apache.sedona.core.kcpJudgement.KCPQueryUtils;
import org.apache.sedona.core.kcpJudgement.NestedLoopJudgement;
import org.apache.sedona.core.monitoring.Metric;
import org.apache.sedona.core.monitoring.Metrics;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRDD.CircleRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.utils.GeomUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.random.SamplingUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.*;

public class KCPQuery
{
    private static final Logger log = LogManager.getLogger(KCPQuery.class);

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

    private static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> collectGeometriesByKey(JavaPairRDD<U, T> input)
    {
        return input.groupBy(t -> GeomUtils.hashCode(t._1)).values().<U, List<T>>mapToPair(t -> {
            List<T> values = new ArrayList<T>();
            Iterator<Tuple2<U, T>> it = t.iterator();
            Tuple2<U, T> firstTpl = it.next();
            U key = firstTpl._1;
            values.add(firstTpl._2);
            while (it.hasNext()) { values.add(it.next()._2); }
            return new Tuple2<U, List<T>>(key, values);
        });
    }

    private static <U extends Geometry, T extends Geometry> JavaPairRDD<U, Long> countGeometriesByKey(JavaPairRDD<U, T> input)
    {
        return input.aggregateByKey(
                0L,
                new Function2<Long, T, Long>()
                {

                    @Override
                    public Long call(Long count, T t)
                            throws Exception
                    {
                        return count + 1;
                    }
                },
                new Function2<Long, Long, Long>()
                {

                    @Override
                    public Long call(Long count1, Long count2)
                            throws Exception
                    {
                        return count1 + count2;
                    }
                });
    }

    /**
     * Inner joins two sets of geometries on 'contains' or 'intersects' relationship.
     * <p>
     * If {@code considerBoundaryIntersection} is {@code true}, returns pairs of geometries
     * which intersect. Otherwise, returns pairs of geometries where first geometry contains second geometry.
     * <p>
     * If {@code useIndex} is false, the join uses nested loop algorithm to identify matching geometries.
     * <p>
     * If {@code useIndex} is true, the join scans query windows and uses an index of geometries
     * built prior to invoking the join to lookup matches.
     * <p>
     * Duplicate geometries present in the input queryWindowRDD, regardless of their non-spatial attributes, will not be reflected in the join results.
     * Duplicate geometries present in the input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join results.
     * @param <U> Type of the geometries in queryWindowRDD set
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries which serve as query windows
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param k number of closest pairs
     * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> List<DistanceAndPair<U,T>> KClosestPairsQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, boolean useIndex, int k, KCPQAlgorithm algorithm, Double sample)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, k, algorithm, sample);
        final PriorityQueue<DistanceAndPair<U, T>> joinResults = kClosestPairs(queryRDD, spatialRDD, joinParams);
        return priorityQueueToList(joinResults);
    }

    public static <U extends Geometry, T extends Geometry> List<DistanceAndPair<U,T>> KClosestPairsQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams)
            throws Exception
    {
        final PriorityQueue<DistanceAndPair<U, T>> joinResults = kClosestPairs(queryRDD, spatialRDD, joinParams);
        return priorityQueueToList(joinResults);
    }

    private static <T extends Geometry, U extends Geometry> List<DistanceAndPair<U,T>> priorityQueueToList(PriorityQueue<DistanceAndPair<U,T>> joinResults) {
        List<DistanceAndPair<U,T>> finalResult = new ArrayList<>();
        while(joinResults.size()>0){
           finalResult.add(joinResults.pop());
        }
        return finalResult;
    }

    /**
     * <p>
     * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT IT IS.
     * </p>
     */
    public static <U extends Geometry, T extends Geometry> PriorityQueue<DistanceAndPair<U, T>> kClosestPairs(
            SpatialRDD<U> queryRDD,
            SpatialRDD<T> spatialRDD,
            JoinParams joinParams)
            throws Exception
    {

        SparkContext sparkContext = spatialRDD.spatialPartitionedRDD.context();
        Metric buildCount = Metrics.createMetric(sparkContext, "buildCount");
        Metric streamCount = Metrics.createMetric(sparkContext, "streamCount");
        Metric resultCount = Metrics.createMetric(sparkContext, "resultCount");
        Metric candidateCount = Metrics.createMetric(sparkContext, "candidateCount");

        final SpatialPartitioner partitioner =
                (SpatialPartitioner) spatialRDD.spatialPartitionedRDD.partitioner().get();
        final DedupParams dedupParams = partitioner.getDedupParams();

        final JavaRDD<PriorityQueue<DistanceAndPair<U, T>>> firstJoinResult, joinResult;

        verifyCRSMatch(queryRDD, spatialRDD);

        queryRDD.rawSpatialRDD.cache();

        PriorityQueue<DistanceAndPair<U, T>> boundPq = calculateBound(queryRDD, spatialRDD, joinParams.k, joinParams.algorithm, joinParams.sample);

        final Double bound = boundPq.top().distance;

        System.out.println("Bound: "+bound);

        queryRDD.spatialPartitioning(partitioner);

        verifyPartitioningMatch(queryRDD, spatialRDD);

        boolean intersects = queryRDD.boundaryEnvelope == null ? true : spatialRDD.boundaryEnvelope.intersects(queryRDD.boundaryEnvelope);

        if(intersects) {

            NestedLoopJudgement judgement = new NestedLoopJudgement(joinParams.k, joinParams.algorithm, bound, dedupParams);
            judgement.broadcastDedupParams(sparkContext);
            firstJoinResult = spatialRDD.spatialPartitionedRDD.zipPartitions(queryRDD.spatialPartitionedRDD, judgement);

            boundPq = firstJoinResult.fold(null, KCPQueryUtils::foldHeaps);

        }

        final Double secondBound = boundPq.top().distance;

        System.out.println("Second Bound: "+secondBound);

        if(secondBound == 0) {
            System.out.println("Final Bound: "+secondBound);
            return boundPq;
        }

        if(intersects) {
            spatialRDD.spatialPartitionedRDD = spatialRDD.spatialPartitionedRDD.mapPartitionsWithIndex(filterBounds(dedupParams, secondBound), true);
            queryRDD.rawSpatialRDD = queryRDD.spatialPartitionedRDD.mapPartitionsWithIndex(filterBounds(dedupParams, secondBound), true);
        }

        CircleRDD circleRDD = new CircleRDD(queryRDD, secondBound);
        circleRDD.spatialPartitioning(partitioner);
        JavaRDD<T> pointRDD = circleRDD.spatialPartitionedRDD.map(circle -> (T)circle.getCenterGeometry());

        NestedLoopJudgement judgement = new NestedLoopJudgement(joinParams.k, joinParams.algorithm, bound, true, dedupParams);
        judgement.broadcastDedupParams(sparkContext);

        joinResult = spatialRDD.spatialPartitionedRDD.zipPartitions(pointRDD, judgement);

        PriorityQueue<DistanceAndPair<U, T>> result = joinResult.fold(null, (heapA, heapB) -> KCPQueryUtils.foldHeaps(heapA, heapB));

        result = KCPQueryUtils.foldHeaps(boundPq, result);

        System.out.println("Final Bound: "+result.top().distance);

        return result;
    }

    private static <T extends Geometry> Function2<Integer, Iterator<T>, Iterator<T>> filterBounds(DedupParams dedupParams, Double secondBound) {
        return (partitionId, tIterator) -> {
            Envelope extent = dedupParams.getPartitionExtents().get(partitionId);

            boolean fullX = extent.getWidth() < secondBound;
            boolean fullY = extent.getHeight() < secondBound;

            Envelope margins = new Envelope(extent.getMinX()+ secondBound, extent.getMaxX()- secondBound, extent.getMinY()+ secondBound, extent.getMaxY()- secondBound);
            List<T> results = new ArrayList<T>();
            while(tIterator.hasNext()){
                T next = tIterator.next();
                if(fullX || fullY)
                    results.add(next);
                else if(!margins.contains(next.getEnvelopeInternal()))
                    results.add(next);
            }
            return results.iterator();
            };
    }

    private static <U extends Geometry, T extends Geometry> PriorityQueue<DistanceAndPair<U,T>> calculateBound(SpatialRDD<U> queryRDD, SpatialRDD<T> spatialRDD, int numberK, KCPQAlgorithm algorithm, Double sample) {
        List<U> querySamples = sample(queryRDD, numberK, sample);
        List<T> spatialSamples = sample(spatialRDD, numberK, sample);

        PriorityQueue<DistanceAndPair<U,T>> pq = KCPQueryUtils.reverseKCPQuery(querySamples, spatialSamples, numberK, algorithm, null, null);
        return pq;
    }

    private static <T extends Geometry> List<T> sample(SpatialRDD<T> spatialRDD, int k, Double sample){
        final double fraction = sample; //SamplingUtils.computeFractionForSampleSize((int) Math.max(Math.round(spatialRDD.approximateTotalCount * sample), k), spatialRDD.approximateTotalCount, false);
        return spatialRDD.rawSpatialRDD.sample(false, fraction, 1).sortBy(t -> t.getCentroid().getX(), true, 1)
                .collect();
    }

    public static final class JoinParams
    {
        public final boolean useIndex;
        public final int k;
        public final IndexType indexType;
        public final JoinBuildSide joinBuildSide;
        public final KCPQAlgorithm algorithm;
        private final Double sample;

        public JoinParams(boolean useIndex, int k, KCPQAlgorithm algorithm, Double sample)
        {
            this(useIndex, k, algorithm, sample, IndexType.RTREE, JoinBuildSide.RIGHT);
        }

        public JoinParams(boolean useIndex, int k, KCPQAlgorithm algorithm, Double sample, IndexType polygonIndexType, JoinBuildSide joinBuildSide)
        {
            this.useIndex = useIndex;
            this.k = k;
            this.algorithm = algorithm;
            this.sample = sample;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
        }
        
    }
}

