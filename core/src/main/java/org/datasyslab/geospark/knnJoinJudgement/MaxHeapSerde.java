package org.datasyslab.geospark.knnJoinJudgement;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.log4j.Logger;
import org.datasyslab.geospark.geometryObjects.SpatialIndexSerde;

public class MaxHeapSerde extends Serializer {

    private static final Logger log = Logger.getLogger(SpatialIndexSerde.class);

    private GeometryWithDistanceSerde geometryWithDistanceSerde;

    public MaxHeapSerde()
    {
        super();
        geometryWithDistanceSerde = new GeometryWithDistanceSerde();
    }

    public MaxHeapSerde(GeometryWithDistanceSerde geometryWithDistanceSerde)
    {
        super();
        this.geometryWithDistanceSerde = geometryWithDistanceSerde;
    }

    @Override
    public void write(Kryo kryo, Output output, Object o) {
        MaxHeap<Geometry> heap = (MaxHeap) o;
        output.writeInt(heap.getK());
        output.writeInt(heap.size());
        for(GeometryWithDistance geometry : heap){
            geometryWithDistanceSerde.write(kryo,output,geometry);
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        int k = input.readInt();
        MaxHeap<Geometry> heap = new MaxHeap<>(k);
        int size = input.readInt();
        for(int n = 0; n < size; n++){
            heap.add((GeometryWithDistance<Geometry>) geometryWithDistanceSerde.read(kryo, input, aClass));
        }

        return heap;
    }
}
