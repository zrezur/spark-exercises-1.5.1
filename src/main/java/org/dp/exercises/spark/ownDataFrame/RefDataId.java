package org.dp.exercises.spark.ownDataFrame;

import java.io.Serializable;

/**
 * Created by Y700-17 on 12.12.2016.
 */
public class RefDataId implements Serializable {

    private long id;

    public RefDataId() {
    }

    public RefDataId(long id) {
        this.id = id;
    }

    public Long getIdentity(){
        return id;
    }
}
