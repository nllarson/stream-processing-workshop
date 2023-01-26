package org.improving.workshop.utopia;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EventTicket {
    private Event event;
    private Ticket ticket;

}
