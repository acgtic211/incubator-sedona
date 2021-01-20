package org.datasyslab.geospark.knnJoinJudgement;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.*;
import org.apache.log4j.Logger;
import org.datasyslab.geospark.geometryObjects.GeometrySerde;

public class GeometryWithDistanceSerde extends GeometrySerde
{

    private static final Logger log = Logger.getLogger(GeometrySerde.class);

    @Override
    public void write(Kryo kryo, Output output, Object o) {
        GeometryWithDistance geom = (GeometryWithDistance) o;
        super.write(kryo, output, geom.geometry);
        output.writeDouble(geom.distance);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        Geometry geometry = (Geometry) super.read(kryo, input, aClass);
        double distance = input.readDouble();
        GeometryWithDistance geom = new GeometryWithDistance();
        geom.geometry = geometry;
        geom.distance = distance;

        return geom;
    }
}
