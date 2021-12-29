package org.apache.sedona.core.enums;

public enum KCPQAlgorithm {
    CLASSIC (0),
    CLASSIC_Y (1),
    CLASSIC_FULL (2),
    REVERSE (3),
    REVERSE_Y (4),
    REVERSE_FULL (5);

    public final int value;
    private KCPQAlgorithm(int value) {
        this.value = value;
    }

}
