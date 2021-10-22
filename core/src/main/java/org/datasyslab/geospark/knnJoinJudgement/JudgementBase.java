/*
 * FILE: JudgementBase
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
package org.datasyslab.geospark.knnJoinJudgement;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.utils.HalfOpenRectangle;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

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
    private static final Logger log = LogManager.getLogger(JudgementBase.class);

    private final boolean considerBoundaryIntersection;
    private final DedupParams dedupParams;
    protected final int k;
    private final SpatialPartitioner partitioner;

    transient private HalfOpenRectangle extent;

    /**
     * @param considerBoundaryIntersection true for 'intersects', false for 'contains' join condition
     * @param dedupParams Optional information to activate de-dup logic
     * @param partitioner
     * @param k
     */
    protected JudgementBase(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams, SpatialPartitioner partitioner, int k)
    {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.dedupParams = dedupParams;
        this.partitioner = partitioner;
        this.k = k ;
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

        final List<Envelope> partitionExtents = dedupParams.getPartitionExtents();
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
                return geoMatch(left, right);
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

        return geoMatch(left, right);
    }

    private Point makePoint(double x, double y, GeometryFactory factory)
    {
        return factory.createPoint(new Coordinate(x, y));
    }

    private boolean geoMatch(Geometry left, Geometry right)
    {
        //log.warn("Check "+left.toText()+" with "+right.toText());
        return considerBoundaryIntersection ? left.intersects(right) : left.covers(right);
    }

    protected boolean contains(Geometry geometry){
        if(extent != null){
            return extent.contains(geometry.getCentroid());
        }
        return false;
    }

    protected boolean isFinal(Geometry streamShape, double maxDistance) {
        boolean isFinal = true;
        if(maxDistance == Double.NEGATIVE_INFINITY)
            return true;
        final Circle circle = new Circle(streamShape, maxDistance);
        final List<Envelope> partitions = this.partitioner.getGrids();
        final int thisPartition = TaskContext.getPartitionId();
        for(int n = 0; n < partitions.size(); n++) {
            if(thisPartition == n)
                continue;
            Envelope grid = partitions.get(n);
            if (circle.getEnvelopeInternal().intersects(grid)) {
                isFinal = false;
                break;
            }
        }
        return isFinal;
    }

    protected KnnData<Geometry> calculateKnnData(STRtree treeIndex, Geometry streamShape, GeometryItemDistance geometryItemDistance, boolean checkOverlaps) {

        Object[] topk = null;

        try{
            topk = treeIndex.kNearestNeighbour(streamShape.getEnvelopeInternal(), streamShape, geometryItemDistance, this.k);
            List<Geometry> localK = new ArrayList<>(topk.length);

            double maxDistance = Double.NEGATIVE_INFINITY;

            for (int i = 0; i < topk.length; i++) {
                maxDistance = Math.max(maxDistance,streamShape.distance((Geometry) topk[i]));
                localK.add((Geometry) topk[i]);
            }

            boolean isFinal = !checkOverlaps || isFinal(streamShape, maxDistance);

            return new KnnData<>(localK, isFinal, maxDistance);

        }catch (Exception e){
            return new KnnData<>(new ArrayList<>(), true, Double.NEGATIVE_INFINITY);
        }



    }

    protected KnnData<Geometry> calculateKnnData(List<Geometry> trainingObjects, Geometry streamShape, boolean checkOverlaps) {
        PriorityQueue<Geometry> pq = new PriorityQueue<Geometry>(k, new GeometryDistanceComparator(streamShape, false));
        double maxDistance = Double.NEGATIVE_INFINITY;
        for (Geometry curpoint : trainingObjects) {
            double distance = curpoint.distance(streamShape);
            if (pq.size() < k) {
                pq.offer(curpoint);
                maxDistance = Math.max(maxDistance, distance);
            }
            else {
                if (maxDistance > distance) {
                    pq.poll();
                    pq.offer(curpoint);
                }
                maxDistance = pq.peek().distance(streamShape);
            }
        }
        ArrayList<Geometry> res = new ArrayList<>();
        while(!pq.isEmpty()){
            res.add(pq.poll());
        }

        boolean isFinal = !checkOverlaps || isFinal(streamShape, maxDistance);
        return new KnnData<>(res, isFinal, maxDistance);
    }

    public KnnData<Geometry> calculateKnnDataSorted(List<Geometry> trainingObjects, Geometry streamShape, boolean checkOverlaps, Double distances) {
        PriorityQueue<Geometry> pq = new PriorityQueue<Geometry>(k, new GeometryDistanceComparator(streamShape, false));

        double maxDistance = Double.NEGATIVE_INFINITY;

        final Geometry r = streamShape;
        Envelope rect_r = r.getEnvelopeInternal();
        List<Geometry> S = trainingObjects;
        if(trainingObjects.size()>0) {
            int left = 0;
            int right = S.size() - 1;
            int m = 0;
            Geometry s;
            Envelope rect_s;
            while (left <= right) {
                m = (left + right) / 2;
                //    	  If Am = T, the search is done; return m.
                //    			  If Am < T, set L to m + 1 and go to step 2.
                //    			  If Am > T, set R to m - 1 and go to step 2.
                s = S.get(m);
                rect_s = s.getEnvelopeInternal();

                if (rect_s.getMinX() == rect_r.getMinX()) {
                    break;
                } else {
                    if (rect_s.getMinX() < rect_r.getMinX()) {
                        left = m + 1;
                    } else {
                        right = m - 1;
                    }
                }

            }

            int pl, pr;

            if (m < 0) {
                m = 0;
                pl = m;
                pr = m + 1;
            } else if (m > S.size() - 1) {
                m = S.size() - 1;
                pl = m - 1;
                pr = m;
            } else {
                pl = m;
                pr = m + 1;
            }

            double gdmax = Double.POSITIVE_INFINITY;
            if(distances!=null){
                gdmax = distances;
            }
            boolean flagPL = pl < 0;
            boolean flagPR = pr > S.size() - 1;

            while (!flagPL || !flagPR) {
                if (!flagPL) {
                    s = S.get(pl);
                    rect_s = s.getEnvelopeInternal();

                    double dx = Math.abs(rect_r.getMinX() - rect_s.getMinX());
                    if (dx > gdmax || (dx == gdmax && pq.size()==k)) {
                        flagPL = true;
                        continue;
                    }

                    double dy = Math.abs(rect_r.getMinY() - rect_s.getMinY());
                    if (dy > gdmax || (dy == gdmax && pq.size()==k)) {
                        pl--;
                        flagPL = pl < 0;
                        continue;
                    }

                    double dist = rect_r.distance(rect_s);
                    if(dist < gdmax || (dist == gdmax && !(pq.size()==k))){
                        pq.offer(s);
                        if(pq.size()>k){
                            pq.poll();
                        }
                        if (pq.size()==k) {
                            gdmax = rect_r.distance(pq.peek().getEnvelopeInternal());
                        }
                    }

                    pl--;
                    flagPL = pl < 0;
                }

                if (!flagPR) {
                    s = S.get(pr);
                    rect_s = s.getEnvelopeInternal();
                    double dx = Math.abs(rect_r.getMinX() - rect_s.getMinX());

                    if (dx > gdmax || (dx == gdmax && pq.size()==k)) {
                        flagPR = true;
                        continue;
                    }

                    double dy = Math.abs(rect_r.getMinY() - rect_s.getMinY());
                    if (dy > gdmax || (dy == gdmax && pq.size()==k)) {
                        pr++;
                        flagPR = pr > S.size() - 1;
                        continue;
                    }

                    double dist = rect_r.distance(rect_s);
                    if(dist < gdmax || (dist == gdmax && !(pq.size()==k))){
                        pq.offer(s);
                        if(pq.size()>k){
                            pq.poll();
                        }
                        if (pq.size()==k) {
                            gdmax = rect_r.distance(pq.peek().getEnvelopeInternal());
                        }
                    }

                    pr++;
                    flagPR = pr > S.size() - 1;
                }

            }

        }

        if(pq.isEmpty())
            return new KnnData<>(new ArrayList<>(), true, Double.NEGATIVE_INFINITY);

        maxDistance = pq.peek().distance(streamShape);
        ArrayList<Geometry> res = new ArrayList<>();
        while(!pq.isEmpty()){
            res.add(pq.poll());
        }

        boolean isFinal = !checkOverlaps || isFinal(streamShape, maxDistance);
        return new KnnData<>(res, isFinal, maxDistance);
    }
}
