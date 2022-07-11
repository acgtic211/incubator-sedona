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

package org.apache.sedona.core.knnJoinJudgement;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.GeometryItemDistance;
import org.locationtech.jts.index.strtree.STRtree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RangeRightIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<Circle>, Iterator<SpatialIndex>, Pair<T, List<U>>>, Serializable
{

    private final SpatialPartitioner partitioner;

    /**
     * @see JudgementBase
     */
    public RangeRightIndexLookupJudgement(@Nullable DedupParams dedupParams, SpatialPartitioner partitioner, int k)
    {
        super(true, dedupParams, partitioner, k);
        this.partitioner = partitioner;
    }

    @Override
    public Iterator<Pair<T, List<U>>> call(Iterator<Circle> streamShapes, Iterator<SpatialIndex> indexIterator)
            throws Exception
    {
        List<Pair<T, List<U>>> result = new ArrayList<>();

        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            return result.iterator();
        }

        initPartition();

        GeometryItemDistance geometryItemDistance = new GeometryItemDistance();
        SpatialIndex treeIndex = indexIterator.next();
        if (treeIndex instanceof STRtree) {
            while (streamShapes.hasNext()) {
                Circle circle = streamShapes.next();
                T streamShape = (T) circle.getCenterGeometry();

                /*if(contains(streamShape)){
                    continue;
                }*/

                final List knnData = calculateKnnData((STRtree) treeIndex, streamShape, geometryItemDistance, circle.getRadius());

                result.add(Pair.of(streamShape, knnData));
            }

        }
        else {
            throw new Exception("[KnnJudgementUsingIndex][Call] QuadTree index doesn't support KNN search.");
        }
        return result.iterator();
    }

}
