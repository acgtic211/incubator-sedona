package org.datasyslab.geospark.knnJoinJudgement;

import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.HashSet;

public class KnnData<T extends Geometry> implements Serializable {
    public MaxHeap<T> neighbors;
    public boolean isFinal;

    public KnnData(int k)
    {
        neighbors = new MaxHeap<>(k);
        isFinal = false;
    }

    public KnnData(MaxHeap<T> neighbors, boolean isFinal) {
        this.neighbors = neighbors;
        this.isFinal = isFinal;
    }
}
