package org.datasyslab.geospark.knnJoinJudgement;

import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;

public class KnnData<T extends Geometry> implements Serializable {
    public List<T> neighbors;
    public boolean isFinal;
    public double distance;

    public KnnData(List<T> neighbors, boolean isFinal, double distance) {
        this.neighbors = neighbors;
        this.isFinal = isFinal;
        this.distance = distance;
    }
}
