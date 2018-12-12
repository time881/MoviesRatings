package indi.zion.InfoStream.Beans;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class BeanDecoder<T extends Bean> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO Auto-generated method stub
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        // TODO Auto-generated method stub
        return (T) T.toBean(data);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }
}
