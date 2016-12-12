package org.dp.exercises.spark.ownDataFrame;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Entity implements Serializable{

    public Map<String, Object> attributes = new HashMap<>();

    public void addAttribute(String key, Object value){
        attributes.put(key, value);
    }

    public  Object getAttirubute(String key){
        return attributes.get(key);
    }

}
