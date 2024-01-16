package com.playtika.shepherd.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.playtika.shepherd.inernal.utils.BytesUtils.toBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SerDeUtils {

    public static final SerDe<ByteBuffer> BYTE_BUFFER_DE_SER = new SerDe<>() {
        @Override
        public List<ByteBuffer> serialize(List<ByteBuffer> population) {
            return population;
        }

        @Override
        public List<ByteBuffer> deserialize(List<ByteBuffer> assignment) {
            return assignment;
        }
    };

    public static final SerDe<String> STRING_DE_SER = new SerDe<>() {
        @Override
        public List<ByteBuffer> serialize(List<String> population) {
            return population.stream().map(s -> ByteBuffer.wrap(s.getBytes(UTF_8))).toList();
        }

        @Override
        public List<String> deserialize(List<ByteBuffer> assignment) {
            return assignment.stream().map(buffer -> new String(toBytes(buffer), UTF_8)).toList();
        }
    };

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <Breed> SerDe<Breed> getSerDe(Class<Breed> breedClass){

        return new SerDe<>() {
            @Override
            public List<ByteBuffer> serialize(List<Breed> population) {
                return population.stream().map(sheep -> {
                    try {
                        return ByteBuffer.wrap(objectMapper.writeValueAsString(sheep).getBytes());
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }).toList();
            }

            @Override
            public List<Breed> deserialize(List<ByteBuffer> assignment) {
                return assignment.stream().map(buffer -> {
                    try {
                        return objectMapper.readValue(toBytes(buffer), breedClass);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).toList();
            }
        };
    }

}
