package org.apache.sedona.core.knnJoinJudgement;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.apache.sedona.core.geometryObjects.SpatialIndexSerde;

public class KnnDataSerde extends Serializer {

    private static final Logger log = Logger.getLogger(SpatialIndexSerde.class);

    private MaxHeapSerde maxHeapSerde;

    public KnnDataSerde()
    {
        super();
        maxHeapSerde = new MaxHeapSerde();
    }

    public KnnDataSerde(MaxHeapSerde geometryWithDistanceSerde)
    {
        super();
        this.maxHeapSerde = maxHeapSerde;
    }

    @Override
    public void write(Kryo kryo, Output output, Object o) {
        KnnData knnData = (KnnData) o;
        maxHeapSerde.write(kryo, output, knnData.neighbors);
        output.writeBoolean(knnData.isFinal);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        return null;
    }
}
