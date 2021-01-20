package org.apache.sedona.core.knnJoinJudgement;

import org.locationtech.jts.geom.Geometry;
import java.io.Serializable;

public class GeometryWithDistance<T extends Geometry> implements Serializable, Comparable<GeometryWithDistance> {
    public Double distance;
    public T geometry;

    public GeometryWithDistance() {
    }

    public GeometryWithDistance(T geom1, Geometry geom2) {
        distance = geom1.distance(geom2);
        geometry = geom1;
    }

    @Override
    public int compareTo(GeometryWithDistance o) {
        return distance.compareTo(o.distance);
    }

}