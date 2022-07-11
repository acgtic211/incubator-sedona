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

import org.apache.sedona.core.monitoring.Metric;
import org.apache.sedona.core.utils.TimeUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.GeometryItemDistance;
import org.locationtech.jts.index.strtree.STRtree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

public class RightIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<SpatialIndex>, Pair<T, List<U>>>, Serializable
{

    private SpatialPartitioner partitioner;
    private Metric queryCount = null;
    private Metric timeElapsed = null;
    private Metric dataCount = null;

    /**
     * @see JudgementBase
     */
    public RightIndexLookupJudgement(@Nullable DedupParams dedupParams, SpatialPartitioner partitioner, int k, Metric queryCount, Metric dataCount, Metric timeElapsed)
    {
        super(true, dedupParams, partitioner, k);
        this.partitioner = partitioner;
        this.queryCount = queryCount;
        this.dataCount = dataCount;
        this.timeElapsed = timeElapsed;
    }

    @Override
    public Iterator<Pair<T, List<U>>> call(Iterator<T> streamShapes, Iterator<SpatialIndex> indexIterator)
            throws Exception
    {
        List<Pair<T, List<U>>> result = new ArrayList<>();

        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            return result.iterator();
        }

        //long startTime = System.currentTimeMillis();

        initPartition();
        GeometryItemDistance geometryItemDistance = new GeometryItemDistance();
        SpatialIndex treeIndex = indexIterator.next();
        if (treeIndex instanceof STRtree) {
            /*if(dataCount!=null)
                dataCount.add(((STRtree) treeIndex).size());*/
            while (streamShapes.hasNext()) {
                T streamShape = streamShapes.next();

                List<U> knnData = calculateKnnData((STRtree) treeIndex, streamShape, geometryItemDistance, true);

                result.add(Pair.of(streamShape, knnData));
            }
            /*if(queryCount!=null)
                queryCount.add(result.size());

            if(timeElapsed!=null){
                timeElapsed.add(TimeUtils.elapsedSince(startTime));
            }*/

        }
        else {
            throw new Exception("[KnnJudgementUsingIndex][Call] QuadTree index doesn't support KNN search.");
        }
        return result.iterator();
    }
}
