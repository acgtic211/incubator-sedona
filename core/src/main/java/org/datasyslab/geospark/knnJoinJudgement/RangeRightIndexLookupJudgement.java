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
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RangeRightIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<Circle>, Iterator<SpatialIndex>, Pair<T, KnnData<U>>>, Serializable
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
    public Iterator<Pair<T, KnnData<U>>> call(Iterator<Circle> streamShapes, Iterator<SpatialIndex> indexIterator)
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
                Circle circle = streamShapes.next();
                T streamShape = (T) circle.getCenterGeometry();

                if(contains(streamShape)){
                    continue;
                }

                KnnData<U> knnData = (KnnData<U>) calculateKnnData((STRtree) treeIndex, streamShape, geometryItemDistance, false);

                result.add(Pair.of(streamShape, knnData));
            }

        }
        else {
            throw new Exception("[KnnJudgementUsingIndex][Call] QuadTree index doesn't support KNN search.");
        }
        return result.iterator();
    }

}
