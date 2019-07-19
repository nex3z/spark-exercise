package com.nex3z.examples.spark.stream.passengercount;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class Passenger {

    private String shopName;

    private String passengerId;

    private int age;

    private String arriveTime;

}
