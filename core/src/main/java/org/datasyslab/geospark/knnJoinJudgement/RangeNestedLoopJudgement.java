/*
 * FILE: NestedLoopJudgement
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class RangeNestedLoopJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<Circle>, Iterator<U>, Pair<T, KnnData<U>>>, Serializable
{
    private static final Logger log = LogManager.getLogger(RangeNestedLoopJudgement.class);

    /**
     * @see JudgementBase
     */
    public RangeNestedLoopJudgement(@Nullable DedupParams dedupParams, SpatialPartitioner partitioner, int k)
    {
        super(true, dedupParams, partitioner, k);
    }

    @Override
    public Iterator<Pair<T, KnnData<U>>> call(Iterator<Circle> iteratorObject, Iterator<U> iteratorTraining)
            throws Exception
    {
        initPartition();

        List<Pair<T, KnnData<U>>> result = new ArrayList<>();
        List<Geometry> trainingObjects = new ArrayList<>();
        while (iteratorTraining.hasNext()) {
            trainingObjects.add(iteratorTraining.next());
        }

        Comparator<Geometry> comparator = new Comparator<Geometry>() {
            @Override
            public int compare(Geometry o1, Geometry o2) {
                Envelope rect_o1, rect_o2;
                rect_o1 = o1.getEnvelopeInternal();
                rect_o2 = o2.getEnvelopeInternal();
                if (rect_o1.getMinX() == rect_o2.getMinX())
                    return 0;
                return rect_o1.getMinX() < rect_o2.getMinX() ? -1 : 1;
            }
        };


        trainingObjects.sort(comparator);

        while (iteratorObject.hasNext()) {
            Circle circle = iteratorObject.next();
            T streamShape = (T) circle.getCenterGeometry();

            if(contains(streamShape)){
                continue;
            }

            KnnData<U> knnData = (KnnData<U>) calculateKnnDataSorted(trainingObjects, streamShape, false, circle.getRadius());
            if(knnData.distance != Double.NEGATIVE_INFINITY)
                result.add(Pair.of(streamShape, knnData));
        }
        return result.iterator();
    }

}
