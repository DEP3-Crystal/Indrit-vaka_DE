package com.crystal.apache.schema.model;

import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
@Data
@Builder
@DefaultSchema(JavaFieldSchema.class)
public class SalesRecord {
    @SchemaIgnore
    public final long id = Math.round(Math.random());
    @SchemaFieldName("date")
    public final String dateTime;
    public final String product;
    public final float price;
    public final String paymentType;
    public final String country;

    @SchemaCreate
    public SalesRecord(String dateTime, String product, float price, String paymentType, String country) {
        this.dateTime = dateTime;
        this.product = product;
        this.price = price;
        this.paymentType = paymentType;
        this.country = country;
    }

    @Override
    public String toString() {
        return dateTime + ',' +
                product + ',' +
                price +
                paymentType + ',' +
                country + ',';
    }
}
