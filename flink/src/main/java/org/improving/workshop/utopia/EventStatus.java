package org.improving.workshop.utopia;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class EventStatus {
    private Event event;
    private double totalRequested;
    private double remaining;

    // stores only the most recently requested ticket
    private Ticket currentTicketRequest;

    public EventStatus(Event event) {
        this.event = event;
        this.totalRequested = 0;
        this.remaining = event.getCapacity();
    }

    public void decrementRemainingTickets(Ticket ticket) {
        setCurrentTicketRequest(ticket);
        this.totalRequested++;
        this.remaining = this.remaining - 1;
    }

    public boolean hasRemainingTickets() {
        return remaining >= 0;
    }
}
