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

import org.apache.hadoop.util.PriorityQueue;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.KCPQAlgorithm;
import org.apache.sedona.core.kcpJudgement.DistanceAndPair;
import org.apache.sedona.core.kcpJudgement.KCPQueryUtils;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.utils.TimeUtils;
import org.apache.spark.api.java.function.Function2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PointKCPQTest
        extends JoinTestBase
{

    private static long expectedRectangleMatchCount;
    private static long expectedRectangleMatchWithOriginalDuplicatesCount;
    private static long expectedPolygonMatchCount;
    private static long expectedPolygonMatchWithOriginalDuplicatesCount;

    int k;

    public PointKCPQTest(GridType gridType, int numPartitions, int k)
    {
        super(gridType, numPartitions);
        this.k = k;
    }

    @Parameterized.Parameters
    public static Collection testParams()
    {
        return Arrays.asList(new Object[][] {
                {GridType.KDBTREE, 10, 10},
                //{GridType.QUADTREE, 10, 10},
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
    public void test()
            throws Exception
    {
        PointRDD queryRDD = createQueryRDD();
        testIndexInt(queryRDD, null, false, expectedRectangleMatchCount);


        local();
    }

    private DistanceAndPair<Point, Point> local() {
        PointRDD queryRDD2 = createQueryRDD();
        PointRDD spatialRDD = createPointRDD();

        List<Point> query = queryRDD2.rawSpatialRDD.sortBy(t -> t.getCentroid().getX(), true, 1)
                .collect();
        List<Point> spatial = spatialRDD.rawSpatialRDD.sortBy(t -> t.getCentroid().getX(), true, 1)
                .collect();

        PriorityQueue<DistanceAndPair<Point, Point>> result = KCPQueryUtils.reverseKCPQuery(spatial, query, this.k, KCPQAlgorithm.REVERSE_FULL, null, null);
        return result.top();
    }

    private void testIndexInt(SpatialRDD<Point> queryRDD, IndexType indexType, boolean useIndex, long expectedCount)
            throws Exception
    {
        long startTime = System.currentTimeMillis();

        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);

        if(indexType!=null)
            spatialRDD.buildIndex(indexType, true);

        List<DistanceAndPair<Point, Point>> result = KCPQuery.KClosestPairsQuery(spatialRDD, queryRDD, false, this.k, KCPQAlgorithm.REVERSE_FULL, 0.001);
        assertEquals(k, result.size());
        System.out.println(TimeUtils.elapsedSince(startTime));
        startTime = System.currentTimeMillis();
        assertEquals(result.get(0).toString(), local().toString());
        System.out.println(TimeUtils.elapsedSince(startTime));

    }

    private PointRDD createPointRDD()
    {
        return createPointRDD(InputLocation);
    }
    private PointRDD createQueryRDD()
    {
        return createPointRDD(InputLocationQueryWindow);
    }
}