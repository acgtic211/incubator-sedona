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
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

public class NestedLoopJudgement<T extends Geometry, U extends Geometry>
        extends org.apache.sedona.core.kcpJudgement.JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<U>, PriorityQueue<DistanceAndPair<T,U>>>, Serializable
{
    private static final Logger log = LogManager.getLogger(NestedLoopJudgement.class);

    /**
     * @see org.apache.sedona.core.kcpJudgement.JudgementBase
     */
    public NestedLoopJudgement(int k, KCPQAlgorithm algorithm, Double beta, @Nullable DedupParams dedupParams)
    {
        super(k, algorithm, beta, false, dedupParams);
    }

    public NestedLoopJudgement(int k, KCPQAlgorithm algorithm, Double beta, boolean filter, DedupParams dedupParams) {
        super(k, algorithm, beta, filter, dedupParams);
    }

    @Override
    public Iterator<PriorityQueue<DistanceAndPair<T,U>>> call(Iterator<T> iteratorObject, Iterator<U> iteratorWindow)
            throws Exception
    {
        if (!iteratorObject.hasNext() || !iteratorWindow.hasNext()) {
            return Collections.emptyIterator();
        }

        initPartition();

        final boolean doFilter = this.filter;

        List<PriorityQueue<DistanceAndPair<T,U>>> result = new ArrayList<>();
        List<U> queryObjects = getQueryObjects(iteratorWindow, doFilter);
        List<T> spatialObjects = getSpatialObjects(iteratorObject);

        PriorityQueue<DistanceAndPair<T,U>> pq = this.reverseKCPQuery(spatialObjects, queryObjects, kpairs, algorithm, beta, null);

        result.add(pq);
        
        return result.iterator();
    }

    private List<T> getSpatialObjects(Iterator<T> iteratorWindow) {
        List<T> spatialObjects = new ArrayList<>();
        while (iteratorWindow.hasNext()) {
            spatialObjects.add(iteratorWindow.next());
        }
        return spatialObjects;
    }

    private List<U> getQueryObjects(Iterator<U> iteratorObject, boolean doFilter) {
        List<U> queryObjects = new ArrayList<>();
        while (iteratorObject.hasNext()) {
            U next = iteratorObject.next();

            if(doFilter){
                if(contains(next))
                    continue;

            }

            queryObjects.add(next);

        }
        return queryObjects;
    }
}
