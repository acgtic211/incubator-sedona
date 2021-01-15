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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class RangeRightIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<Circle>, Iterator<SpatialIndex>, Pair<T, KnnData<U>>>, Serializable
{

    /**
     * @see JudgementBase
     */
    public RangeRightIndexLookupJudgement(@Nullable DedupParams dedupParams, int k)
    {
        super(true, dedupParams, k);
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

        SpatialIndex treeIndex = indexIterator.next();
        if (treeIndex instanceof STRtree) {
            while (streamShapes.hasNext()) {
                Circle streamShape = streamShapes.next();
                T point = (T) streamShape.getCenterGeometry();
                final MaxHeap<U> localK = new MaxHeap<U>(k);
                Object[] topk = ((STRtree) treeIndex).kNearestNeighbour(streamShape.getEnvelopeInternal(), point, new GeometryItemDistance(), this.k);
                for (int i = 0; i < topk.length; i++) {
                    GeometryWithDistance<U> geom = new GeometryWithDistance<U>((U)topk[i], streamShape);
                    localK.add(geom);
                }
                result.add(Pair.of(point, new KnnData<U>(localK, true)));
            }
        }
        else {
            throw new Exception("[KnnJudgementUsingIndex][Call] QuadTree index doesn't support KNN search.");
        }
        return result.iterator();
    }
}
