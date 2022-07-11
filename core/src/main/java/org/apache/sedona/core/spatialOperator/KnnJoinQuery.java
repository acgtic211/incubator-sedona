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
package org.apache.sedona.core.spatialOperator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.rdd.RDD;
import org.locationtech.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.core.monitoring.Metric;
import org.apache.sedona.core.monitoring.Metrics;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.knnJoinJudgement.*;
import org.apache.sedona.core.knnJudgement.GeometryDistanceComparator;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class KnnJoinQuery {
    private static final Logger log = LogManager.getLogger(KnnJoinQuery.class);

    private static <U extends Geometry, T extends Geometry> void verifyCRSMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD)
            throws Exception {
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
            throws Exception {
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

    public static final class JoinParams {
        public final boolean useIndex;
        public final boolean considerBoundaryIntersection;
        public final boolean allowDuplicates;
        public final IndexType indexType;
        public final JoinBuildSide joinBuildSide;
        public final GridType gridType;

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection, boolean allowDuplicates, GridType gridType) {
            this.useIndex = useIndex;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = allowDuplicates;
            this.indexType = IndexType.RTREE;
            this.joinBuildSide = JoinBuildSide.RIGHT;
            this.gridType = gridType;
        }

        public JoinParams(boolean considerBoundaryIntersection, IndexType polygonIndexType, JoinBuildSide joinBuildSide) {
            this.useIndex = false;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = false;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
            this.gridType = GridType.QUADTREE;
        }
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> KnnJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, int k, boolean useIndex, boolean considerBoundaryIntersection, GridType gridType)
            throws Exception {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, true, gridType);
        final JavaPairRDD<U, List<T>> joinResults = knnJoin(queryRDD, spatialRDD, k, joinParams);
        return joinResults;
    }

    /**
     * <p>
     * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT IT IS.
     * </p>
     *
     * @return
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> knnJoin(
            SpatialRDD<U> leftRDD,
            SpatialRDD<T> rightRDD,
            int k,
            JoinParams joinParams)
            throws Exception {

        verifyCRSMatch(leftRDD, rightRDD);
        verifyPartitioningMatch(leftRDD, rightRDD);

        SparkContext sparkContext = leftRDD.spatialPartitionedRDD.context();
        Metric buildCount = Metrics.createMetric(sparkContext, "buildCount");
        Metric streamCount = Metrics.createMetric(sparkContext, "streamCount");
        Metric resultCount = Metrics.createMetric(sparkContext, "resultCount");
        Metric candidateCount = Metrics.createMetric(sparkContext, "candidateCount");
        Metric queryCount = Metrics.createMetric(sparkContext, "queryCount");
        Metric dataCount = Metrics.createMetric(sparkContext, "dataCount");
        Metric timeElapsed = Metrics.createMetric(sparkContext, "timeElapsed");

        final SpatialPartitioner partitioner =
                (SpatialPartitioner) rightRDD.spatialPartitionedRDD.partitioner().get();
        final DedupParams dedupParams = partitioner.getDedupParams();

        final JavaRDD<Pair<U, List<T>>> resultWithDuplicates;
        JavaRDD<Pair<U, List<T>>> finalKnnLists, nonFinalKnnLists, nonFinalKnnLists2;
        final RightIndexLookupJudgement judgement =
                new RightIndexLookupJudgement(dedupParams, partitioner, k, queryCount, dataCount, timeElapsed);
        resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement);
        /*if (joinParams.useIndex) {
            if (rightRDD.indexedRDD != null) {
                final RightIndexLookupJudgement judgement =
                        new RightIndexLookupJudgement(dedupParams, partitioner, k, queryCount, dataCount, timeElapsed);
                resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement);

            } /*else {
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
        } /*else {
            final NestedLoopJudgement judgement =
                    new NestedLoopJudgement(dedupParams, partitioner, k);
            resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
        }*/


        final JavaRDD<Pair<U, List<T>>> temp = resultWithDuplicates.persist(StorageLevel.MEMORY_ONLY());

        final JavaRDD<Pair<U, Tuple2<List<T>, Long>>> withDistancesAndFinal = getWithDistancesAndFinal(partitioner, temp);

        JavaRDD<Pair<U, Tuple2<List<T>, Long>>> nonFinalWithDistancesAndFinal = withDistancesAndFinal.filter(pair -> pair.getValue()._2 != 0);


        List<Pair<Integer, Long>> skewedPartitions = calculateSkewedPartitions(leftRDD, nonFinalWithDistancesAndFinal);
        skewedPartitions.forEach(pair -> System.out.println("Partition: " + pair.getKey() + " Count: " + pair.getValue()));

        nonFinalKnnLists = nonFinalWithDistancesAndFinal.map(pair -> Pair.of(pair.getKey(), pair.getValue()._1));
        finalKnnLists = withDistancesAndFinal.filter(pair -> pair.getValue()._2 == 0).map(pair -> Pair.of(pair.getKey(), pair.getValue()._1));
        ;

        HashSet<Integer> skewedPartitionSet = new HashSet<>();
        skewedPartitions.forEach(pair -> skewedPartitionSet.add(pair.getKey()));

        JavaRDD<Pair<U, List<T>>> nonFinalKnnListsSkewed = filterSkewedPartitions(skewedPartitionSet, nonFinalKnnLists, true);
        JavaRDD<Pair<U, List<T>>> nonFinalKnnListsNonSkewed = filterSkewedPartitions(skewedPartitionSet, nonFinalKnnLists, false);

        JavaRDD<Circle> circlesNonSkewed = toCircles(nonFinalKnnListsNonSkewed);
        SpatialRDD<Circle> spatialCirclesNonSkewed = new SpatialRDD<>();

        spatialCirclesNonSkewed.rawSpatialRDD = circlesNonSkewed;
        spatialCirclesNonSkewed.spatialPartitioning(partitioner);

        final RangeRightIndexLookupJudgement judgement2 =
                new RangeRightIndexLookupJudgement(dedupParams, partitioner, k);

        JavaRDD<Pair<U, List<T>>> nonFinalKnnListsNonSkewed2 = spatialCirclesNonSkewed.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement2);

        JavaRDD<Circle> circlesSkewed = toCircles(nonFinalKnnListsSkewed);

        SpatialRDD<Circle> spatialCirclesSkewed = new SpatialRDD<>();
        spatialCirclesSkewed.rawSpatialRDD = circlesSkewed;
        spatialCirclesSkewed.spatialPartitioning(partitioner);


        JavaRDD<Circle> circles = toCircles(nonFinalKnnLists);

        SpatialRDD<Circle> spatialCircles = new SpatialRDD<>();
        spatialCircles.rawSpatialRDD = circles;
        spatialCircles.spatialPartitioning(GridType.KDBTREE);

        rightRDD.spatialPartitioning(spatialCircles.getPartitioner());
        rightRDD.buildIndex(IndexType.RTREE, true);

        final RangeRightIndexLookupJudgement judgement3 =
                new RangeRightIndexLookupJudgement(dedupParams, spatialCircles.getPartitioner(), k);

        JavaRDD<Pair<U, List<T>>> nonFinalKnnListsSkewed2 = spatialCircles.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement3);

        nonFinalKnnLists2 = nonFinalKnnListsSkewed2.union(nonFinalKnnListsNonSkewed2);

        JavaPairRDD<U, Pair<U, List<T>>> nonFinalKnnData = nonFinalKnnLists.union(nonFinalKnnLists2).mapToPair((PairFunction<Pair<U, List<T>>, U, Pair<U, List<T>>>) pair -> Tuple2.apply(pair.getKey(), Pair.of(pair.getKey(), pair.getValue())));

        JavaPairRDD<U, List<T>> nonFinalKnnResults = nonFinalKnnData.combineByKey(
                uKnnDataPair -> {
                    PriorityQueue<T> pq = new PriorityQueue<T>(k, new GeometryDistanceComparator(uKnnDataPair.getLeft(), false));
                    for (T curpoint : uKnnDataPair.getRight()) {
                        pq.offer(curpoint);
                    }
                    return pq;
                },
                (pq, pair) -> {
                    U actual = pair.getLeft();
                    for (T curpoint : pair.getRight()) {
                        if (pq.size() == k) {
                            if (pq.peek().distance(actual) > curpoint.distance(actual)) {
                                pq.poll();
                                pq.offer(curpoint);
                            }
                            pq.poll();
                        } else {
                            pq.offer(curpoint);
                        }

                    }
                    return pq;
                },
                (pq, pq_other) -> {
                    while (!pq_other.isEmpty()) {
                        if (pq.size() == k) {
                            if (pq.comparator().compare(pq.peek(), pq_other.peek()) > 0) {
                                pq.poll();
                                pq.offer(pq_other.poll());
                            } else {
                                pq_other.poll();
                            }
                        } else {
                            pq.offer(pq_other.poll());
                        }
                    }
                    return pq;
                }
                , finalKnnLists.getNumPartitions() / 2).mapToPair((PairFunction<Tuple2<U, PriorityQueue<T>>, U, List<T>>) pair -> new Tuple2<U, List<T>>(pair._1(), new ArrayList<T>(pair._2)));

        JavaPairRDD<U, List<T>> finalKnnResults = finalKnnLists.mapToPair((PairFunction<Pair<U, List<T>>, U, List<T>>) pair -> new Tuple2<U, List<T>>(pair.getKey(), pair.getValue()));

        JavaPairRDD<U, List<T>> result = finalKnnResults.union(nonFinalKnnResults);

        return result;

    }

    private static <U extends Geometry, T extends Geometry> JavaRDD<Pair<U, List<T>>> filterSkewedPartitions(HashSet<Integer> skewedPartitionSet, JavaRDD<Pair<U, List<T>>> nonFinalKnnLists, boolean isSkewed) {
        return PartitionPruningRDD.create(nonFinalKnnLists.rdd(), new PartitionPruningFunction(skewedPartitionSet, isSkewed)).toJavaRDD();
    }

    private static <U extends Geometry, T extends Geometry> JavaRDD<Circle> toCircles(JavaRDD<Pair<U, List<T>>> nonFinalKnnLists) {
        return nonFinalKnnLists.map(pair -> {
            final List objects = pair.getValue();
            final double maxDistance = pair.getKey().distance((Geometry) objects.get(objects.size() - 1));
            return new Circle(pair.getKey(), maxDistance);
        });
    }

    private static <U extends Geometry, T extends Geometry> JavaRDD<Pair<U, Tuple2<List<T>, Long>>> getWithDistancesAndFinal(SpatialPartitioner partitioner, JavaRDD<Pair<U, List<T>>> temp) {
        return temp.map(pair -> {
            final List<T> objects = pair.getValue();

            if (objects.size() == 0) {
                return Pair.of(pair.getKey(), new Tuple2<>(objects, 0L));
            }

            final double maxDistance = pair.getKey().distance((Geometry) objects.get(objects.size() - 1));

            final Circle circle = new Circle(pair.getKey(), maxDistance);

            Long finalCount = 0L;

            Iterator<Tuple2<Integer, Circle>> overlaps = partitioner.placeObject(circle);

            while (overlaps.hasNext()) {
                overlaps.next();
                finalCount++;
            }

            return Pair.of(pair.getKey(), Tuple2.apply(objects, finalCount));
        });
    }

    private static <U extends Geometry, T extends Geometry> List<Pair<Integer, Long>> calculateSkewedPartitions(SpatialRDD<U> leftRDD, JavaRDD<Pair<U, Tuple2<List<T>, Long>>> nonFinalKnnListsFiltered) {
        List<Pair<Integer, Long>> elementsPerPartition = nonFinalKnnListsFiltered.mapPartitionsWithIndex((index, iterator) -> {
            long count = 0;
            while (iterator.hasNext()) {
                Long actualCount = iterator.next().getValue()._2;
                count += actualCount;
            }
            return Collections.singletonList(Pair.of(index, count)).iterator();
        }, true).collect();

        List<Pair<Integer, Long>> elementsPerPartition2 = leftRDD.spatialPartitionedRDD.mapPartitionsWithIndex((index, iterator) -> {
            long count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return Collections.singletonList(Pair.of(index, count)).iterator();
        }, true).collect();

        //Join elementsPerPartition and elementsPerPartition2
        List<Pair<Integer, Long>> elementsPerPartitionJoined = new ArrayList<>();
        for (Pair<Integer, Long> pair : elementsPerPartition) {
            for (Pair<Integer, Long> pair2 : elementsPerPartition2) {
                if (pair.getKey() == pair2.getKey()) {
                    elementsPerPartitionJoined.add(Pair.of(pair.getKey(), pair.getValue() + pair2.getValue()));
                }
            }
        }

        //Calculate std dev
        double sum = 0;
        for (Pair<Integer, Long> pair : elementsPerPartitionJoined) {
            sum += pair.getValue();
        }
        double mean = sum / elementsPerPartitionJoined.size();

        double sumOfSquares = 0;
        for (Pair<Integer, Long> pair : elementsPerPartitionJoined) {
            sumOfSquares += Math.pow(pair.getValue() - mean, 2);
        }

        double stdDev = Math.sqrt(sumOfSquares / elementsPerPartitionJoined.size());

        //Calculate threshold
        double threshold = mean + stdDev;

        //Filter elementsPerPartitionJoined sorted by value
        List<Pair<Integer, Long>> elementsPerPartitionJoinedFiltered = elementsPerPartitionJoined.stream()
                .sorted(Comparator.comparing(Pair::getValue))
                .filter(pair -> pair.getValue() > threshold)
                .limit(elementsPerPartitionJoined.size() / 4)
                .collect(Collectors.toList());

        System.out.println("Threshold: " + threshold);
        return elementsPerPartitionJoinedFiltered;
    }

    private static class PartitionPruningFunction extends AbstractFunction1<Object, Object> implements Serializable {
        private static final long serialVersionUID = -9114299718258329951L;

        private HashSet<Integer> _filterFlags = null;
        private boolean _filterIn = true;

        public PartitionPruningFunction(HashSet<Integer> flags, boolean filterIn) {
            _filterFlags = flags;
            _filterIn = filterIn;
        }

        @Override
        public Boolean apply(Object partIndex) {
            if (this._filterIn) {
                return _filterFlags.contains(partIndex);
            } else {
                return !_filterFlags.contains(partIndex);
            }
        }
    }
}

