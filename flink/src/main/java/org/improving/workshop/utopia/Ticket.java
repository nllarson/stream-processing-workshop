package org.improving.workshop.utopia;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Ticket {
    private String id;
    private String customerid;
    private String eventid;
    private Double price;
}
