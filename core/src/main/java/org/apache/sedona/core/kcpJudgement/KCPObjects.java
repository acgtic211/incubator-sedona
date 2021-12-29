package org.apache.sedona.core.kcpJudgement;

import java.io.Serializable;

/**
 * Keeps KNN objects ordered by their distance descending
 *
 * @author Ahmed Eldawy
 *
 */
public class KCPObjects<S extends Comparable<S>> extends org.apache.hadoop.util.PriorityQueue<S> implements Serializable {

    private int capacity;

    public KCPObjects(int k) {
        this.capacity = k;
        super.initialize(k);
    }

    /**
     * Keep elements sorted in descending order (Max heap)
     */
    @Override
    protected boolean lessThan(Object a, Object b) {
        return ((S) a).compareTo((S) b) > 0;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public boolean isFull() {
        return this.size()>=this.capacity;
    }
}
