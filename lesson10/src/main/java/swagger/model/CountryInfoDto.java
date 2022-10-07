/*
 * Nager.Date API - V3
 * Nager.Date is open source software and is completely free for commercial use. If you would like to support the project you can award a GitHub star ⭐ or even better <a href='https://github.com/sponsors/nager'>actively support us</a>
 *
 * OpenAPI spec version: v3
 *
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */

package swagger.model;

import com.google.gson.annotations.SerializedName;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * CountryInfo Dto
 */
@Schema(description = "CountryInfo Dto")
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.JavaClientCodegen", date = "2022-09-12T09:30:37.127Z[GMT]")
public class CountryInfoDto {
    @SerializedName("commonName")
    private String commonName = null;

    @SerializedName("officialName")
    private String officialName = null;

    @SerializedName("countryCode")
    private String countryCode = null;

    @SerializedName("region")
    private String region = null;

    @SerializedName("borders")
    private List<CountryInfoDto> borders = null;

    public CountryInfoDto commonName(String commonName) {
        this.commonName = commonName;
        return this;
    }

    /**
     * CommonName
     *
     * @return commonName
     **/
    @Schema(description = "CommonName")
    public String getCommonName() {
        return commonName;
    }

    public void setCommonName(String commonName) {
        this.commonName = commonName;
    }

    public CountryInfoDto officialName(String officialName) {
        this.officialName = officialName;
        return this;
    }

    /**
     * OfficialName
     *
     * @return officialName
     **/
    @Schema(description = "OfficialName")
    public String getOfficialName() {
        return officialName;
    }

    public void setOfficialName(String officialName) {
        this.officialName = officialName;
    }

    public CountryInfoDto countryCode(String countryCode) {
        this.countryCode = countryCode;
        return this;
    }

    /**
     * CountryCode
     *
     * @return countryCode
     **/
    @Schema(description = "CountryCode")
    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public CountryInfoDto region(String region) {
        this.region = region;
        return this;
    }

    /**
     * Region
     *
     * @return region
     **/
    @Schema(description = "Region")
    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public CountryInfoDto borders(List<CountryInfoDto> borders) {
        this.borders = borders;
        return this;
    }

    public CountryInfoDto addBordersItem(CountryInfoDto bordersItem) {
        if (this.borders == null) {
            this.borders = new ArrayList<CountryInfoDto>();
        }
        this.borders.add(bordersItem);
        return this;
    }

    /**
     * Country Borders
     *
     * @return borders
     **/
    @Schema(description = "Country Borders")
    public List<CountryInfoDto> getBorders() {
        return borders;
    }

    public void setBorders(List<CountryInfoDto> borders) {
        this.borders = borders;
    }


    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CountryInfoDto countryInfoDto = (CountryInfoDto) o;
        return Objects.equals(this.commonName, countryInfoDto.commonName) &&
                Objects.equals(this.officialName, countryInfoDto.officialName) &&
                Objects.equals(this.countryCode, countryInfoDto.countryCode) &&
                Objects.equals(this.region, countryInfoDto.region) &&
                Objects.equals(this.borders, countryInfoDto.borders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonName, officialName, countryCode, region, borders);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class CountryInfoDto {\n");

        sb.append("    commonName: ").append(toIndentedString(commonName)).append("\n");
        sb.append("    officialName: ").append(toIndentedString(officialName)).append("\n");
        sb.append("    countryCode: ").append(toIndentedString(countryCode)).append("\n");
        sb.append("    region: ").append(toIndentedString(region)).append("\n");
        sb.append("    borders: ").append(toIndentedString(borders)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }

}
