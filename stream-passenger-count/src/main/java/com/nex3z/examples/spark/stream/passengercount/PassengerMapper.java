package com.nex3z.examples.spark.stream.passengercount;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.MapFunction;

public class PassengerMapper implements MapFunction<String, Passenger> {
    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public Passenger call(String s) throws Exception {
        return mapper.readValue(s, Passenger.class);
    }
}
