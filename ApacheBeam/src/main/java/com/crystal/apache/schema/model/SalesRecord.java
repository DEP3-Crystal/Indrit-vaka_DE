package com.crystal.apache.schema.model;

import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;

import java.util.Objects;
@Data
@DefaultSchema(JavaFieldSchema.class)
public class SalesRecord {
    @SchemaIgnore
    private final long id = Math.round(Math.random());
    @SchemaFieldName("date")
    private final String dateTime;
    private final String product;
    private final float price;
    private final String paymentType;
    private final String country;

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
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        SalesRecord that = (SalesRecord) o;
//        return id == that.id && Float.compare(that.price, price) == 0 && Objects.equals(dateTime, that.dateTime) && Objects.equals(product, that.product) && Objects.equals(paymentType, that.paymentType) && Objects.equals(country, that.country);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(id, dateTime, product, price, paymentType, country);
//    }
}
