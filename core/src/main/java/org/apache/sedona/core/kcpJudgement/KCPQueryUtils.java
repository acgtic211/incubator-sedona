package org.apache.sedona.core.kcpJudgement;

import org.apache.hadoop.util.PriorityQueue;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.KCPQAlgorithm;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.List;

public class KCPQueryUtils {

    private static final Logger log = LogManager.getLogger(KCPQueryUtils.class);

    /**
     *
     * Finds Top K Closest Pairs using Reverse Plain Sweep technique
     *
     * @param p1 Ordered Shape array
     * @param q1 Ordered Shape array
     * @param numberK Number of elements needed for Top k
     * @param algorithm Number of pruning algorithm (classic, rectangle, circle)
     * @param beta Pre-filtered beta value
     * @param alpha alpha allowance value
     * @return Top K Closest Pairs
     */
    public static <T extends Geometry, U extends Geometry> PriorityQueue<DistanceAndPair<T,U>> reverseKCPQuery(List<T> p1, List<U> q1, int numberK, KCPQAlgorithm algorithm, Double beta, Float alpha) {

        int i = 0, j = 0, k, stk; // local counter of the points
        Double gdmax = beta != null ? beta : -1;
        Double distance;
        float fAlpha = alpha != null ? alpha : 0.0f;
        KCPObjects<DistanceAndPair<T,U>> kcpho = new KCPObjects<>(numberK);
        PriorityQueue<DistanceAndPair<T,U>> ho = kcpho;
        DistanceAndPair<T,U> aux = new DistanceAndPair<T,U>();
        Point refo, curo;
        long leftp = -1, leftq = -1;
        int gtotPoints1 = p1.size(), gtotPoints2 = q1.size();
        GeometryFactory geometryFactory = new GeometryFactory();

        Geometry[] p = new Geometry[gtotPoints1 + 1];
        p1.toArray(p);
        p[gtotPoints1] = geometryFactory.createPoint(new Coordinate(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));

        Geometry[] q = new Geometry[gtotPoints2 + 1];
        q1.toArray(q);
        q[gtotPoints2] = geometryFactory.createPoint(new Coordinate(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));

        log.info(gtotPoints1 + ":" + gtotPoints2);

        // first find the most left point of two datasets
        if (p[0].getCentroid().getX() < q[0].getCentroid().getX()) { // if P[0] < Q[0]
            // if the datasets have no intersection
            if (p[gtotPoints1 - 1].getCentroid().getX() <= q[0].getCentroid().getX()) { // if
                // P[last]<=Q[0]
                i = gtotPoints1; // the LEFT scan begins from i-1 index
                // j = 0L; // the Q set is on the right side
            }
        } else { // else if the most left point is from set Q
            // if the datasets have no intersection
            if (q[gtotPoints2 - 1].getCentroid().getX() <= p[0].getCentroid().getX()) { // if
                // Q[last]<=P[0]
                j = gtotPoints2; // the LEFT scan begins from j-1 index
                // i = 0L; // the P set is on the right side
            }
        }

        // if we have intersection between the two datasets
        // 1. while the points of both sets are not finished
        for (;;) {

            if (p[i].getCentroid().getX() < q[j].getCentroid().getX()) { // if the subset is from
                // the set P
                // 2. for each P[i] while i < P.N and P[i].getX() < Q[j].getX() increment
                // i
                stk = j - 1;
                do {

                    // 3. if j-1 = leftq then continue // this run was
                    // interrupted
                    if (stk == leftq) {
                        continue;
                    }
                    // 4. set ref_point = P[i]
                    refo = p[i].getCentroid();
                    k = stk;
                    // 5. for k=j-1 to leftq decrement k
                    do {
                        // 6. set cur_point = Q[k]
                        curo = q[k].getCentroid();

                        if (!kcpho.isFull() && beta == null) {

                            distance = refo.distance(curo);
                            aux.set((T)p[i], (U)q[k], distance);
                            ho.insert(aux.clone());
                            gdmax = ho.top().distance*(1.0f-fAlpha);

                        } else {

                            distance = calculateDistance(refo,curo,gdmax,kcpho.isFull(),algorithm);

                            if(distance==null){
                                leftq = k;
                                break;
                            }else if(distance<0){
                                continue;
                            }

                            if (distance < gdmax || (distance == gdmax && !kcpho.isFull())) {
                                aux.set((T)p[i], (U)q[k], distance);
                                ho.insert(aux.clone());
                                gdmax = ho.top().distance * (1.0f - fAlpha);
                            }

                        }

                    } while (--k > leftq); // next point of the set Q
                } while ((p[++i].getCentroid().getX() < q[j].getCentroid().getX())); // next i :
                // next
                // point
                // from the
                // set P if
                // the run
                // continues

            } else if (j < gtotPoints2) { // else if the set Q is not finished
                p[gtotPoints1] = geometryFactory.createPoint(new Coordinate(q[gtotPoints2 - 1].getCentroid().getX() + 1, p[gtotPoints1].getCentroid().getY()));

                // 18. for each Q[j] while j < Q.N and Q[j] <= P[i] increment j
                stk = i - 1;
                do {
                    // 19. if i-1 = leftp then continue // this run was
                    // interrupted
                    if (stk == leftp) {
                        continue;
                    }
                    // 20. set ref_point = Q[j]
                    refo = q[j].getCentroid();
                    k = stk;
                    // 21. for k=i-1 to leftp decrement k
                    do {
                        // 22. set cur_point = P[k]
                        curo = p[k].getCentroid();
                        if (!kcpho.isFull() && beta == null) {

                            distance = refo.distance(curo);
                            aux.set((T)p[k], (U)q[j], distance);
                            ho.insert(aux.clone());
                            gdmax = ho.top().distance*(1.0f-fAlpha);

                        } else {

                            distance = calculateDistance(refo,curo,gdmax,kcpho.isFull(),algorithm);

                            if(distance==null){
                                leftp = k;
                                break;
                            }else if(distance<0){
                                continue;
                            }

                            if (distance < gdmax || (distance == gdmax && !kcpho.isFull())) {
                                aux.set((T)p[k], (U)q[j], distance);
                                ho.insert(aux.clone());
                                gdmax = ho.top().distance * (1.0f - fAlpha);
                            }

                        }

                    } while (--k > leftp); // next point of the set P
                } while (q[++j].getCentroid().getX() <= p[i].getCentroid().getX()); // next j :
                // next
                // point
                // from the
                // set Q if
                // the run
                // continues
                // revert the max value in the P[last].getX()
                p[gtotPoints1] = geometryFactory.createPoint(new Coordinate(q[gtotPoints2].getCentroid().getX(), p[gtotPoints1].getCentroid().getY()));
                // P[gtotPoints1].m[0]=Q[gtotPoints2].m[0];
            } else {
                break; // the process is finished
            }

        } // loop while(i < gtotPoints1 || j < gtotPoints2)

        return ho;
    }

    private static Double calculateDistance(Point refo, Point curo, double gdmax, boolean full, KCPQAlgorithm algorithm){

        double dx, dy, ssdx, gdmax2;
        double distance;
        int realAlgorithm = (algorithm.value) % 3;

        dx = Math.abs(curo.getX() - refo.getX());
        if (dx > gdmax || (dx == gdmax && full)) {
            return null;
        }

        if(realAlgorithm==0) {
            distance = refo.distance(curo);
            return distance;
        }

        dy = Math.abs(curo.getY() - refo.getY());
        if (dy > gdmax || (dy == gdmax && full)) {
            distance = -1.0d;
            return distance;
        }

        if(realAlgorithm==1) {
            distance = refo.distance(curo);
            return distance;
        }

        ssdx = dx * dx;
        ssdx += dy * dy;
        gdmax2 = gdmax * gdmax;
        if (ssdx < gdmax2 || (ssdx == gdmax && !full)) {
            distance = Math.sqrt(ssdx);
            return distance;
        }

        return Double.MAX_VALUE;

    }

    public static <U extends Geometry, T extends Geometry> PriorityQueue<DistanceAndPair<U,T>> foldHeaps(PriorityQueue<DistanceAndPair<U,T>> heapA, PriorityQueue<DistanceAndPair<U,T>> heapB) {

        if (heapA==null||heapB==null){
            if(heapA!=null)
                return heapA;
            return heapB;
        }

        while(heapB.size()>0){
            heapA.insert(heapB.pop());
        }

        return heapA;
    }

    public static <T extends Geometry, U extends Geometry> PriorityQueue<DistanceAndPair<T,U>> naiveKCPQuery(List<T> p1, List<U> q1, int numberK) {
        PriorityQueue<DistanceAndPair<T,U>> ho = new KCPObjects<>(numberK);;
        DistanceAndPair<T,U> aux = new DistanceAndPair<T,U>();
        for(T p : p1){
            for(U q: q1) {
                aux.set(p, q, p.distance(q));
                ho.insert(aux.clone());
            }
        }
        return ho;
    }
}
