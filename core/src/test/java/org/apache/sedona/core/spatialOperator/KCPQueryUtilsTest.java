package org.apache.sedona.core.spatialOperator;

import org.apache.hadoop.util.PriorityQueue;
import org.apache.sedona.core.enums.KCPQAlgorithm;
import org.apache.sedona.core.kcpJudgement.DistanceAndPair;
import org.apache.sedona.core.kcpJudgement.KCPQueryUtils;
import org.apache.sedona.core.utils.TimeUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.*;

public class KCPQueryUtilsTest {

    private static final int NUMBER_OF_ELEMENTS = 10000;
    private static final int K = 100;
    private static List<Geometry> p, q;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        p = generatePoints(NUMBER_OF_ELEMENTS);
        q = generatePoints(NUMBER_OF_ELEMENTS);
        Collections.sort(p, Comparator.comparingDouble(lhs -> lhs.getCentroid().getX()));
        Collections.sort(q, Comparator.comparingDouble(lhs -> lhs.getCentroid().getX()));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
    }

    /**
     * kCPQuery
     */
    @Test
    public void kCPQuery() {

        long startTime = System.currentTimeMillis();
        PriorityQueue<DistanceAndPair<Geometry, Geometry>> plane = KCPQueryUtils.reverseKCPQuery(p, q, K, KCPQAlgorithm.REVERSE_FULL, null, null);
        System.out.println(TimeUtils.elapsedSince(startTime));

        startTime = System.currentTimeMillis();
        PriorityQueue<DistanceAndPair<Geometry, Geometry>> naive = KCPQueryUtils.naiveKCPQuery(p, q, K);
        System.out.println(TimeUtils.elapsedSince(startTime));



        while(plane.size()>0) {
            assert plane.pop().distance.equals(naive.pop().distance);
        }
    }

    private static List<Geometry> generatePoints(int elements) {
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope mbr = new Envelope(0, 10000, 0, 10000);
        List<Geometry> list = new ArrayList<>(elements);
        for (int n = 0; n < elements; n++) {
            list.add(geometryFactory.createPoint(randomCoordinates(mbr)));
        }
        return list;
    }

    private static Coordinate randomCoordinates(Envelope mbr) {
        double x = Math.random() * mbr.getWidth() + mbr.getMinX();
        double y = Math.random() * mbr.getHeight() + mbr.getMinY();

        return new Coordinate(x, y);
    }
}
