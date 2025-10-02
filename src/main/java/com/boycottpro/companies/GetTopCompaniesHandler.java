package com.boycottpro.companies;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.boycottpro.models.Companies;
import com.boycottpro.models.CompanySubset;
import com.boycottpro.utilities.CompanyUtility;
import com.boycottpro.utilities.JwtUtility;
import com.boycottpro.utilities.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class GetTopCompaniesHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final String TABLE_NAME = "";
    private final DynamoDbClient dynamoDb;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GetTopCompaniesHandler() {
        this.dynamoDb = DynamoDbClient.create();
    }

    public GetTopCompaniesHandler(DynamoDbClient dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        String sub = null;
        int lineNum = 44;
        try {
            sub = JwtUtility.getSubFromRestEvent(event);
            if (sub == null) {
            Logger.error(48, sub, "user is Unauthorized");
            return response(401, Map.of("message", "Unauthorized"));
        }
            lineNum = 51;
            Map<String, String> pathParams = event.getPathParameters();
            int limit =  Integer.parseInt(pathParams.get("limit"));
            if (limit < 1 ) {
                Logger.error(56, sub, "no limit");
                return response(400,Map.of("error", "Missing limit in path"));
            }
            lineNum = 58;
            List<CompanySubset> companies = getTopCompanies(limit);
            lineNum = 60;
            return response(200,companies);
        } catch (Exception e) {
            Logger.error(lineNum, sub, e.getMessage());
            return response(500,Map.of("error", "Unexpected server error: " + e.getMessage()));
        }
    }

    private APIGatewayProxyResponseEvent response(int status, Object body) {
        String responseBody = null;
        try {
            responseBody = objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(status)
                .withHeaders(Map.of("Content-Type", "application/json"))
                .withBody(responseBody);
    }

    private List<CompanySubset> getTopCompanies(int limit) {
        ScanRequest scan = ScanRequest.builder()
                .tableName("companies")
                .projectionExpression("company_id, company_name, boycott_count")
                .build();
        LocalDateTime start = LocalDateTime.now();
        ScanResponse response = dynamoDb.scan(scan);
        LocalDateTime end = LocalDateTime.now();
        Duration difference = Duration.between(start, end);
        AtomicInteger rankCounter = new AtomicInteger(1);
        start = LocalDateTime.now();
        List<CompanySubset> companies =  response.items().stream()
                .filter(item -> item.containsKey("boycott_count"))
                .sorted(Comparator.comparingInt(
                        item -> -Integer.parseInt(item.get("boycott_count").n())
                ))
                .limit(limit)
                .map(item -> {
                    CompanySubset subset = CompanyUtility.mapToSubset(item);
                    subset.setRank(rankCounter.getAndIncrement());
                    return subset;
                })
                .collect(Collectors.toList());
        end = LocalDateTime.now();
        difference = Duration.between(start, end);
        return companies;
    }


}