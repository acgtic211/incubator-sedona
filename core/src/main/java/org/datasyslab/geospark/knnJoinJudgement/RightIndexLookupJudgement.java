/*
 * FILE: LeftIndexLookupJudgement
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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

public class RightIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<SpatialIndex>, Pair<T, KnnData<U>>>, Serializable
{

    private SpatialPartitioner partitioner;

    /**
     * @see JudgementBase
     */
    public RightIndexLookupJudgement(@Nullable DedupParams dedupParams, SpatialPartitioner partitioner, int k)
    {
        super(true, dedupParams, k);
        this.partitioner = partitioner;
    }

    @Override
    public Iterator<Pair<T, KnnData<U>>> call(Iterator<T> streamShapes, Iterator<SpatialIndex> indexIterator)
            throws Exception
    {
        List<Pair<T, KnnData<U>>> result = new ArrayList<>();

        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            return result.iterator();
        }

        initPartition();
        GeometryItemDistance geometryItemDistance = new GeometryItemDistance();
        SpatialIndex treeIndex = indexIterator.next();
        if (treeIndex instanceof STRtree) {
            while (streamShapes.hasNext()) {
                T streamShape = streamShapes.next();
                final MaxHeap<U> localK = new MaxHeap<U>(k);
                Object[] topk = ((STRtree) treeIndex).kNearestNeighbour(streamShape.getEnvelopeInternal(), streamShape, geometryItemDistance, this.k);
                for (int i = 0; i < topk.length; i++) {
                    GeometryWithDistance<U> geom = new GeometryWithDistance<U>((U)topk[i], streamShape);
                    localK.add(geom);
                }

                double maxDistance = localK.getMaxDistance();
                boolean isFinal = localK.size() == k;
                if(isFinal) {
                    Circle circle = new Circle(streamShape, maxDistance);
                    for (Envelope grid : this.partitioner.getGrids()) {
                        if (circle.getEnvelopeInternal().intersects(grid)) {
                            isFinal = false;
                            break;
                        }
                    }
                }
                result.add(Pair.of(streamShape, new KnnData<U>(localK, isFinal)));
            }
        }
        else {
            throw new Exception("[KnnJudgementUsingIndex][Call] QuadTree index doesn't support KNN search.");
        }
        return result.iterator();
    }
}
