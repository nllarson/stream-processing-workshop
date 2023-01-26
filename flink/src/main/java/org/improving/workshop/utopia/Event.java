package org.improving.workshop.utopia;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Event {
    private String id;
    private String artistid;
    private String venueid;
    private Integer capacity;
    private String eventdate;
}
