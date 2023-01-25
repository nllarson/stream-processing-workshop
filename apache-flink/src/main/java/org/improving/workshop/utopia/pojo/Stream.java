package org.improving.workshop.utopia.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class Stream implements Serializable {
    private String id;
    private String customerid;
    private String artistid;
    private String streamtime;

}
