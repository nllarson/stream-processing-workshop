package org.improving.workshop.utopia;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EventTicketConfirmation {
    private TicketConfirmationStatus confirmationStatus;
    private String confirmationid;
    private Ticket ticketRequest;
    private Event event;
    private double remainingTickets;
}
