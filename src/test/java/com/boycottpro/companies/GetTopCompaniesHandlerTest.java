package com.boycottpro.companies;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import com.boycottpro.models.CompanySubset;
import com.boycottpro.utilities.CompanyUtility;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import java.lang.reflect.Field;
import com.fasterxml.jackson.core.JsonProcessingException;

@ExtendWith(MockitoExtension.class)
public class GetTopCompaniesHandlerTest {

    @Mock
    private DynamoDbClient dynamoDb;

    @Mock
    private Context context;

    @InjectMocks
    private GetTopCompaniesHandler handler;

    @Test
    public void testHandleRequestReturnsTopCompanies() throws Exception {
        // Given
        int limit = 2;
        Map<String, String> pathParams = Map.of("limit", String.valueOf(limit));
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        // Path param "s" since client calls /users/s
        event.setPathParameters(pathParams);
        Map<String, AttributeValue> company1 = Map.of(
                "company_id", AttributeValue.fromS("c1"),
                "company_name", AttributeValue.fromS("Apple"),
                "boycott_count", AttributeValue.fromN("25")
        );
        Map<String, AttributeValue> company2 = Map.of(
                "company_id", AttributeValue.fromS("c2"),
                "company_name", AttributeValue.fromS("Amazon"),
                "boycott_count", AttributeValue.fromN("15")
        );

        when(dynamoDb.scan(argThat((ScanRequest r) ->
                r != null && "companies".equals(r.tableName())
        ))).thenReturn(ScanResponse.builder().items(List.of(company1, company2)).build());

        // When
        APIGatewayProxyResponseEvent response = handler.handleRequest(event, context);

        // Then
        assertEquals(200, response.getStatusCode());
        assertTrue(response.getBody().contains("Apple"));
        assertTrue(response.getBody().contains("Amazon"));
        assertTrue(response.getBody().contains("25"));
        assertTrue(response.getBody().contains("15"));
    }

    @Test
    public void testHandleRequestReturns400ForMissingLimit() {
        int limit = 0;
        Map<String, String> pathParams = Map.of("limit", String.valueOf(limit));
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        // Path param "s" since client calls /users/s
        event.setPathParameters(pathParams);

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, context);

        assertEquals(400, response.getStatusCode());
        assertTrue(response.getBody().contains("Missing limit"));
    }

    @Test
    public void testHandleRequestReturns500Exception() {
        int limit = 5;
        Map<String, String> pathParams = Map.of("limit", String.valueOf(limit));
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        // Path param "s" since client calls /users/s
        event.setPathParameters(pathParams);

        when(dynamoDb.scan(any(ScanRequest.class)))
                .thenThrow(RuntimeException.class);

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, context);

        assertEquals(500, response.getStatusCode());
        assertTrue(response.getBody().contains("Unexpected server error"));
    }

    @Test
    public void testDefaultConstructor() {
        // Test the default constructor coverage
        // Note: This may fail in environments without AWS credentials/region configured
        try {
            GetTopCompaniesHandler handler = new GetTopCompaniesHandler();
            assertNotNull(handler);

            // Verify DynamoDbClient was created (using reflection to access private field)
            try {
                Field dynamoDbField = GetTopCompaniesHandler.class.getDeclaredField("dynamoDb");
                dynamoDbField.setAccessible(true);
                DynamoDbClient dynamoDb = (DynamoDbClient) dynamoDbField.get(handler);
                assertNotNull(dynamoDb);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                fail("Failed to access DynamoDbClient field: " + e.getMessage());
            }
        } catch (software.amazon.awssdk.core.exception.SdkClientException e) {
            // AWS SDK can't initialize due to missing region configuration
            // This is expected in Jenkins without AWS credentials - test passes
            System.out.println("Skipping DynamoDbClient verification due to AWS SDK configuration: " + e.getMessage());
        }
    }

    @Test
    public void testUnauthorizedUser() {
        // Test the unauthorized block coverage
        handler = new GetTopCompaniesHandler(dynamoDb);

        // Create event without JWT token (or invalid token that returns null sub)
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        // No authorizer context, so JwtUtility.getSubFromRestEvent will return null

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(401, response.getStatusCode());
        assertTrue(response.getBody().contains("Unauthorized"));
    }

    @Test
    public void testJsonProcessingExceptionInResponse() throws Exception {
        // Test JsonProcessingException coverage in response method by using reflection
        handler = new GetTopCompaniesHandler(dynamoDb);

        // Use reflection to access the private response method
        java.lang.reflect.Method responseMethod = GetTopCompaniesHandler.class.getDeclaredMethod("response", int.class, Object.class);
        responseMethod.setAccessible(true);

        // Create an object that will cause JsonProcessingException
        Object problematicObject = new Object() {
            public Object writeReplace() throws java.io.ObjectStreamException {
                throw new java.io.NotSerializableException("Not serializable");
            }
        };

        // Create a circular reference object that will cause JsonProcessingException
        Map<String, Object> circularMap = new HashMap<>();
        circularMap.put("self", circularMap);

        // This should trigger the JsonProcessingException -> RuntimeException path
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            try {
                responseMethod.invoke(handler, 500, circularMap);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                }
                throw new RuntimeException(e.getCause());
            }
        });

        // Verify it's ultimately caused by JsonProcessingException
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof JsonProcessingException,
                "Expected JsonProcessingException, got: " + cause.getClass().getSimpleName());
    }

}

