package com.appdynamics.lambda;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.appdynamics.lambda.dal.CommerceOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.App;
import com.github.javafaker.Faker;

// TODO: Add in AppDynamics imports
import com.appdynamics.serverless.tracers.aws.api.AppDynamics;
import com.appdynamics.serverless.tracers.aws.api.Tracer;
import com.appdynamics.serverless.tracers.aws.api.Transaction;
import com.appdynamics.serverless.tracers.dependencies.com.google.gson.Gson;
import com.appdynamics.serverless.tracers.aws.api.ExitCall;

public class FrontEndHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

	private static final Logger LOG = LogManager.getLogger(FrontEndHandler.class);
	private static final Map<String, Object> CONTROLLER_INFO = SecretsManager.getSecret();

	// TODO: Add variables for the tracer and transaction
	Tracer tracer = null;
	Transaction txn = null;

	@Override
	public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
		LOG.info("received: {}", input);

		ApiGatewayResponse response;

		String path = input.get("path").toString();

		// TODO: Add in code to build tracer and start transaction.
		if (AppDVariablesPresent()) {
			String correlationHeader = "";
			String bt_name = path;

			AppDynamics.Config.Builder configBuilder = new AppDynamics.Config.Builder();
			configBuilder.accountName(FrontEndHandler.CONTROLLER_INFO.get("aws-sandbox-controller-account").toString())
					.controllerAccessKey(
							FrontEndHandler.CONTROLLER_INFO.get("aws-sandbox-controller-key").toString())
					.applicationName(System.getenv("APPDYNAMICS_APPLICATION_NAME"))
					.controllerHost(System.getenv("APPDYNAMICS_CONTROLLER_HOST"))
					.controllerPort(Integer.parseInt(System.getenv("APPDYNAMICS_CONTROLLER_PORT")))
					.tierName(System.getenv("APPDYNAMICS_TIER_NAME"))
					.lambdaContext(context);

			tracer = AppDynamics.getTracer(configBuilder.build());

			if (input.containsKey(Tracer.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER_KEY)) {
				correlationHeader = input.get(Tracer.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER_KEY).toString();
			} else {
				ObjectMapper m = new ObjectMapper();
				Map<String, Object> headers = m.convertValue(input.get("headers"),
						new TypeReference<Map<String, Object>>() {
						});
				if (headers != null && headers.containsKey(Tracer.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER_KEY)) {
					correlationHeader = headers.get(Tracer.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER_KEY).toString();
				}
			}

			txn = tracer.createTransaction(correlationHeader);
			txn.start();
		}

		Faker faker = new Faker();

		if (path.equals("/orders/submit")) {
			CommerceOrder order = new CommerceOrder.Builder().random().build();

			// TODO: Add code for exit call to DynamoDB

			ExitCall db_exit_call = null;
			if (txn != null) {
				HashMap<String, String> db_props = new HashMap<>();
				db_props.put("DESTINATION", System.getenv("ORDERS_TABLE_NAME") + " DynamoDB");
				db_props.put("DESTINATION_TYPE", "Amazon Web Services");
				db_exit_call = txn.createExitCall("CUSTOM", db_props);
				db_exit_call.start();
			}

			try {
				order.save();
				Map<String, Object> order_map = new ObjectMapper().convertValue(order,
						new TypeReference<Map<String, Object>>() {
						});
				Response responseBody = new Response("OrderCreated", order_map);
				response = ApiGatewayResponse.builder().setStatusCode(201).setObjectBody(responseBody)
						.setHeaders(Collections.singletonMap("X-Powered-By", faker.gameOfThrones().character()))
						.build();
			} catch (IOException e) {

				// TODO: Add code to report error for exit call to DynamoDB.
				if (db_exit_call != null) {
					db_exit_call.reportError(e);
				}

				Map<String, Object> error_map = new HashMap<String, Object>();
				error_map.put("error_msg", e.getMessage());
				Response responseBody = new Response("Error", error_map);
				response = ApiGatewayResponse.builder().setStatusCode(500).setObjectBody(responseBody)
						.setHeaders(Collections.singletonMap("X-Powered-By", faker.gameOfThrones().character()))
						.build();
			}

			// TODO: Add code to end exit call to DynamoDB.
			if (db_exit_call != null) {
				db_exit_call.stop();
			}

		} else if (path.equals("/orders/random")) {

			String lambda_to_call = context.getFunctionName().replace("lambda-1", "lambda-2");

			// TODO: Add code for exit call to Lambda
			HashMap<String, String> payload = new HashMap<>();
			ExitCall lambda_exit_call = null;

			if (txn != null) {
				HashMap<String, String> lambda_props = new HashMap<>();
				lambda_props.put("DESTINATION", lambda_to_call);
				lambda_props.put("DESTINATION_TYPE", "LAMBDA");
				lambda_exit_call = txn.createExitCall("CUSTOM", lambda_props);
				String outgoingHeader = lambda_exit_call.getCorrelationHeader();
				lambda_exit_call.start();
				payload.put(Tracer.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER_KEY, outgoingHeader);
			}

			AWSLambda lambdaClient = AWSLambdaClientBuilder.standard().withRegion(System.getenv("AWS_REGION_STR"))
					.build();
			InvokeRequest request = new InvokeRequest().withFunctionName(lambda_to_call)
					.withPayload(new Gson().toJson(payload));

			try {
				InvokeResult result = lambdaClient.invoke(request);
				ByteBuffer payload_buf = result.getPayload();
				String str = StandardCharsets.UTF_8.decode(payload_buf).toString();
				List<Object> results = new ObjectMapper().readValue(str, new TypeReference<List<Object>>() {
				});
				Map<String, Object> resp_map = new HashMap<String, Object>();
				resp_map.put("orders", results);

				Response responseBody = new Response("Success", resp_map);
				response = ApiGatewayResponse.builder().setStatusCode(200).setObjectBody(responseBody)
						.setHeaders(Collections.singletonMap("X-Powered-By", faker.gameOfThrones().character()))
						.build();
			} catch (Throwable e) {

				// TODO: Add code to report error for exit call to Lambda
				if (lambda_exit_call != null) {
					lambda_exit_call.reportError(e);
				}

				Map<String, Object> error_map = new HashMap<String, Object>();
				error_map.put("error_msg", e.getMessage());
				Response responseBody = new Response("Error", error_map);
				response = ApiGatewayResponse.builder().setStatusCode(500).setObjectBody(responseBody)
						.setHeaders(Collections.singletonMap("X-Powered-By", faker.gameOfThrones().character()))
						.build();
			}

			// TODO: Add code to end exit call to Lambda
			if (lambda_exit_call != null) {
				lambda_exit_call.stop();
			}

		} else {

			ThreadLocalRandom rnd = ThreadLocalRandom.current();
			try {
				Thread.sleep(rnd.nextLong(150, 500));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			Response responseBody = new Response("Success", input);
			response = ApiGatewayResponse.builder().setStatusCode(200).setObjectBody(responseBody)
					.setHeaders(Collections.singletonMap("X-Powered-By", faker.gameOfThrones().character())).build();
		}

		// TODO: Add code to end transaction
		if (txn != null) {
			txn.stop();
		}

		return response;

	}

	private boolean AppDVariablesPresent() {
		return !IsNullOrEmpty(System.getenv("APPDYNAMICS_ACCOUNT_NAME"))
				&& !IsNullOrEmpty(System.getenv("APPDYNAMICS_APPLICATION_NAME"))
				&& !IsNullOrEmpty(System.getenv("APPDYNAMICS_CONTROLLER_HOST"))
				&& !IsNullOrEmpty(System.getenv("APPDYNAMICS_SERVERLESS_API_ENDPOINT"));
	}

	private boolean IsNullOrEmpty(String str) {
		return str == null || str.isEmpty();
	}

}
