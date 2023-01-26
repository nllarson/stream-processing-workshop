package org.improving.workshop.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ItemCount {
    private String id;
    private Long count;

    public String toString() {
        return id + " - " + count;
    }
}
