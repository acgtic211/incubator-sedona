package org.datasyslab.geospark.knnJoinJudgement;

import com.vividsolutions.jts.geom.Geometry;

import java.util.HashSet;
import java.util.PriorityQueue;

public class MaxHeap<T extends Geometry> extends PriorityQueue<GeometryWithDistance<T>> {
    private int k = 0;

    public int getK() {
        return k;
    }

    public double getMaxDistance() {
        return peek().distance;
    }

    public double getMinDistance() {
        return minDistance;
    }

    private double minDistance = Double.NEGATIVE_INFINITY;

    public MaxHeap(int k){
        super(k);
        this.k = k;
    }

    @Override
    public boolean add(GeometryWithDistance<T> t) {
        boolean result = false;

        if(this.size()<k){
            result = super.add(t);
            updateMinDistance(t);

        } else if(t.distance < getMaxDistance()){
            result = super.add(t);

            if(this.size()>k){
                poll();
            }
            updateMinDistance(t);
        }

        return result;
    }

    private void updateMinDistance(GeometryWithDistance<T> t){
        if(t.distance < minDistance){
            minDistance = t.distance;
        }
    }

}
