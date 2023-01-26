package org.improving.workshop.utopia;

import lombok.*;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventTicketConfirmation {
    @Setter(AccessLevel.PRIVATE)
    private TicketConfirmationStatus confirmationStatus;
    private String confirmationid;
    private Ticket ticketRequest;
    private EventStatus eventStatus;
    private double remainingTickets;

    private boolean nearSellout() {
        if (remainingTickets < 0) {
            return true;
        }

        return ((remainingTickets / eventStatus.getEvent().getCapacity()) * 100) <= 20;
    }

    public String statusMessage() {
        switch (confirmationStatus) {
            case CONFIRMED:
                if (nearSellout()) {
                    return "Ticket Confirmed. Event " + getEventStatus().getEvent().getId() + " is almost sold out, only " + getRemainingTickets() + " ticket(s) remain.";
                } else {
                    return "Ticket Confirmed.  Enjoy the show!";
                }
            case REJECTED:
                return "Ticket Rejected";
            case SOLD_OUT:
                return "Ticket Rejected. Event " + getEventStatus().getEvent().getId() + " has SOLD OUT!";
            default:
                return "Your ticket status is unknown.";
        }
    }
}
