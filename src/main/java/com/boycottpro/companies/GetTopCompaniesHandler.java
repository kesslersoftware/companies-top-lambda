package com.boycottpro.companies;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.boycottpro.models.Companies;
import com.boycottpro.models.CompanySubset;
import com.boycottpro.utilities.CompanyUtility;
import com.boycottpro.utilities.JwtUtility;
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
        try {
            sub = JwtUtility.getSubFromRestEvent(event);
            if (sub == null) return response(401,
                    Map.of("message", "Unauthorized"));
            LocalDateTime start = LocalDateTime.now();
            Map<String, String> pathParams = event.getPathParameters();
            int limit =  Integer.parseInt(pathParams.get("limit"));
            if (limit < 1 ) {
                return response(400,Map.of("error", "Missing limit in path"));
            }

            List<CompanySubset> companies = getTopCompanies(limit);
            LocalDateTime end = LocalDateTime.now();
            Duration difference = Duration.between(start, end);
            System.out.println("entire method took : " + difference.toMillis() + " milliseconds");
            return response(200,companies);
        } catch (Exception e) {
            System.out.println(e.getMessage() + " for user " + sub);
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
        System.out.println("It took : " + difference.toMillis() + " milliseconds to retrieve records from the database");
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
        System.out.println("It took : " + difference.toMillis() + " milliseconds to sort and limit the results");
        return companies;
    }


}