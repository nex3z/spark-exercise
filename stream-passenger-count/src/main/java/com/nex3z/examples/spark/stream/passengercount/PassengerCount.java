package com.nex3z.examples.spark.stream.passengercount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor @NoArgsConstructor
public class PassengerCount {

    private String shopName;

    private Date start;

    private Date end;

    private int count;

}
