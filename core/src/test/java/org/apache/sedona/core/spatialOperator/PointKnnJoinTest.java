/*
 * FILE: PointJoinTest
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.apache.sedona.core.spatialOperator;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.apache.sedona.core.spatialOperator.KnnJoinQuery;
import org.apache.spark.api.java.function.Function2;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.RectangleRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.Tuple2;

import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PointKnnJoinTest
        extends JoinTestBase
{

    private static long expectedRectangleMatchCount;
    private static long expectedRectangleMatchWithOriginalDuplicatesCount;
    private static long expectedPolygonMatchCount;
    private static long expectedPolygonMatchWithOriginalDuplicatesCount;

    int k;

    public PointKnnJoinTest(GridType gridType, int numPartitions, int k)
    {
        super(gridType, numPartitions);
        this.k = k;
    }

    @Parameterized.Parameters
    public static Collection testParams()
    {
        return Arrays.asList(new Object[][] {
                {GridType.KDBTREE, 10, 10},
        });
    }

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize("PointJoin", "point.test.properties");
        expectedRectangleMatchCount = Long.parseLong(prop.getProperty("rectangleMatchCount"));
        expectedRectangleMatchWithOriginalDuplicatesCount =
                Long.parseLong(prop.getProperty("rectangleMatchWithOriginalDuplicatesCount"));
        expectedPolygonMatchCount = Long.parseLong(prop.getProperty("polygonMatchCount"));
        expectedPolygonMatchWithOriginalDuplicatesCount =
                Long.parseLong(prop.getProperty("polygonMatchWithOriginalDuplicatesCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }



    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithPoints()
            throws Exception
    {
        PointRDD queryRDD = createQueryRDD();
        testIndexInt(queryRDD, IndexType.RTREE, true, expectedRectangleMatchCount);
    }

    private void testIndexInt(SpatialRDD<Point> queryRDD, IndexType indexType, boolean useIndex, long expectedCount)
            throws Exception
    {
        PointRDD spatialRDD = createPointRDD();

        //partitionRdds(queryRDD, spatialRDD);

        spatialRDD.spatialPartitioning(gridType, queryRDD);
        queryRDD.spatialPartitioning(spatialRDD.getPartitioner());

        if(indexType!=null)
            spatialRDD.buildIndex(indexType, true);

        //System.out.println(queryRDD.spatialPartitionedRDD.mapPartitionsWithIndex(elementsPerPartition(), false).collect().toString());

        //System.out.println(spatialRDD.spatialPartitionedRDD.mapPartitionsWithIndex(elementsPerPartition(), false).collect().toString());

        List<Tuple2<Point, List<Point>>> result = KnnJoinQuery.KnnJoinQuery(spatialRDD, queryRDD, k, useIndex, true, gridType).collect();
        System.out.println(countKNNJoinResults(result));
        //sanityCheckJoinResults(result);
        //assertEquals(expectedCount, countJoinResults(result));
    }

    private Function2<Integer, Iterator<Point>, Iterator<Integer>> elementsPerPartition() {
        return (integer, pointIterator) -> {
           int n = 0;
           while(pointIterator.hasNext()) {
               n++;
               pointIterator.next();
           }
           return Collections.singletonList(n).iterator();
        };
    }

    private PointRDD createPointRDD()
    {
        return createPointRDD(InputLocation);
    }

    private PointRDD createQueryRDD() {
        return createPointRDD(InputLocationQueryWindow);
    }


    protected <T extends Geometry, U extends Geometry> long countKNNJoinResults(List<Tuple2<U, List<T>>> results)
    {
        int count = 0;
        for (final Tuple2<U, List<T>> tuple : results) {
            count += tuple._2().size();
        }
        return count;
    }

}