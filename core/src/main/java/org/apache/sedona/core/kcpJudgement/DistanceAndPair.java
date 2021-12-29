package org.apache.sedona.core.kcpJudgement;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.jts.geom.Geometry;

public class DistanceAndPair<U extends Geometry, T extends Geometry> implements Cloneable, Comparable<DistanceAndPair<U,T>> {

    public Double distance;
    public Pair<U,T> pair;

    public DistanceAndPair() {
    }

    public DistanceAndPair(double d, U a, T b) {
        distance = d;
        pair = Pair.of(a,b);
    }

    public DistanceAndPair(DistanceAndPair other) {
        this.copy(other);
    }

    public void copy(DistanceAndPair<U,T> other) {
        distance = other.distance;
        pair = Pair.of((U)other.pair.getLeft().copy(), (T)other.pair.getRight().copy());
    }

    public void set(U refo, T curo, Double distance){
        this.distance = distance!=null?distance:refo.distance(curo);
        pair = Pair.of((U)refo.copy(), (T)curo.copy());
    }

    @Override
    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append(distance);
        str.append("\t");
        str.append(pair.getLeft());
        str.append("\t");
        str.append(pair.getRight());
        str.append("\t");
        return str.toString();
    }

    @Override
    public int compareTo(DistanceAndPair o) {
        return distance.compareTo(o.distance);
    }

    @Override
    public DistanceAndPair<U,T> clone() {
        DistanceAndPair<U,T> c = new DistanceAndPair<U,T>();
        c.copy(this);
        return c;
    }
}