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

package org.apache.sedona.core.kcpJudgement;

import org.apache.hadoop.util.PriorityQueue;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.KCPQAlgorithm;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Base class for partition level join implementations.
 * <p>
 * Provides `match` method to test whether a given pair of geometries satisfies join condition.
 * <p>
 * Supports 'contains' and 'intersects' join conditions.
 * <p>
 * Provides optional de-dup logic. Due to the nature of spatial partitioning, the same pair of
 * geometries may appear in multiple partitions. If that pair satisfies join condition, it
 * will be included in join results multiple times. This duplication can be avoided by
 * (1) choosing spatial partitioning that doesn't allow for overlapping partition extents
 * and (2) reporting a pair of matching geometries only from the partition
 * whose extent contains the reference point of the intersection of the geometries.
 * <p>
 * To achieve (1), call SpatialRDD.spatialPartitioning with a GridType.QUADTREE. At the moment
 * this is the only grid type supported by de-dup logic.
 * <p>
 * For (2), provide `DedupParams` when instantiating JudgementBase object. If `DedupParams`
 * is specified, the implementation of the `match` method assumes that condition (1) holds.
 */
abstract class JudgementBase
        implements Serializable
{
    private interface SerializableSupplier<T> extends Serializable, Supplier<T> {}

    private static final Logger log = LogManager.getLogger(JudgementBase.class);

    protected int kpairs;
    protected KCPQAlgorithm algorithm;
    protected Double beta;
    protected boolean filter;

    // Supplier will return a broadcasted reference if broadcastDedupParams() is called,
    // otherwise a local reference is returned.
    private SerializableSupplier<DedupParams> dedupParams;

    transient private HalfOpenRectangle extent;

    /**
     * @param k number of closest pairs
     * @param b
     * @param dedupParams Optional information to activate de-dup logic
     */
    protected JudgementBase(int k, KCPQAlgorithm algorithm, Double beta, boolean filter, @Nullable DedupParams dedupParams)
    {
        this.kpairs = k;
        this.algorithm = algorithm;
        this.beta = beta;
        this.dedupParams = dedupParams == null ? null : () -> dedupParams;
        this.filter = filter;
    }

    /**
     * Broadcasts <code>dedupParams</code> and replaces the local reference with
     * a reference to the broadcasted variable.
     *
     * Broadcasted variables are deserialized once per executor instead of once per task.
     * Broadcasting can reduce execution time significantly for jobs with a large number of partitions.
     *
     * @param cxt
     */
    public void broadcastDedupParams(SparkContext cxt) {
        if (dedupParams != null) {
            Broadcast<DedupParams> broadcast = new JavaSparkContext(cxt).broadcast(dedupParams.get());
            dedupParams = () -> broadcast.value();
        }
    }

    /**
     * Looks up the extent of the current partition. If found, `match` method will
     * activate the logic to avoid emitting duplicate join results from multiple partitions.
     * <p>
     * Must be called before processing a partition. Must be called from the
     * same instance that will be used to process the partition.
     */
    protected void initPartition()
    {
        if (dedupParams == null) {
            return;
        }

        final int partitionId = TaskContext.getPartitionId();

        final List<Envelope> partitionExtents = dedupParams.get().getPartitionExtents();
        if (partitionId < partitionExtents.size()) {
            extent = new HalfOpenRectangle(partitionExtents.get(partitionId));
        }
        else {
            log.warn("Didn't find partition extent for this partition: " + partitionId);
        }
    }

    protected boolean match(Geometry left, Geometry right)
    {
        if (extent != null) {
            // Handle easy case: points. Since each point is assigned to exactly one partition,
            // different partitions cannot emit duplicate results.
            if (left instanceof Point || right instanceof Point) {
                return true;
            }

            // Neither geometry is a point

            // Check if reference point of the intersection of the bounding boxes lies within
            // the extent of this partition. If not, don't run any checks. Let the partition
            // that contains the reference point do all the work.
            Envelope intersection =
                    left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
            if (!intersection.isNull()) {
                final Point referencePoint =
                        makePoint(intersection.getMinX(), intersection.getMinY(), left.getFactory());
                if (!extent.contains(referencePoint)) {
                    return false;
                }
            }
        }

        return true;
    }

    protected boolean contains(Geometry geom){
        return extent.contains(geom.getCentroid());
    };

    private Point makePoint(double x, double y, GeometryFactory factory)
    {
        return factory.createPoint(new Coordinate(x, y));
    }

    /**
     *
     * Finds Top K Closest Pairs using Reverse Plain Sweep technique
     *
     * @param p1 Ordered Shape array
     * @param q1 Ordered Shape array
     * @param numberK Number of elements needed for Top k
     * @param algorithm Number of pruning algorithm (classic, rectangle, circle)
     * @param beta Pre-filtered beta value
     * @param alpha alpha allowance value
     * @return Top K Closest Pairs
     */
    protected <T extends Geometry, U extends Geometry> PriorityQueue<DistanceAndPair<T,U>> reverseKCPQuery(List<T> p1, List<U> q1, int numberK, KCPQAlgorithm algorithm, Double beta, Float alpha) {
        Collections.sort(p1, Comparator.comparingDouble(lhs -> lhs.getCentroid().getX()));
        Collections.sort(q1, Comparator.comparingDouble(lhs -> lhs.getCentroid().getX()));
        return KCPQueryUtils.reverseKCPQuery(p1, q1, numberK, algorithm, beta, alpha);

    }

}
