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

package swagger.auth;


import swagger.Pair;

import java.util.List;
import java.util.Map;

@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.JavaClientCodegen", date = "2022-09-12T09:30:37.127Z[GMT]")
public class OAuth implements Authentication {
    private String accessToken;

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    @Override
    public void applyToParams(List<Pair> queryParams, Map<String, String> headerParams) {
        if (accessToken != null) {
            headerParams.put("Authorization", "Bearer " + accessToken);
        }
    }
}
