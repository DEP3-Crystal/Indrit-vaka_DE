package com.crystal.apache.fixed_window.model;

import lombok.Data;
import org.joda.time.Instant;

import java.io.Serializable;

@Data
public class EnergyConsumption implements Serializable {
    private static final String[] FILE_HEADERS = {
            "Datetime", "AEP_MW"
    };
    private Instant dateTime;
    private Double energyConsumption;

    private static String getCSVHeader() {
        return String.join(",", "DateTime", "MW");
    }

    public String asCSVRow(String delimiter) {
        return String.join(delimiter, this.dateTime.toString(), this.energyConsumption.toString());
    }
}
