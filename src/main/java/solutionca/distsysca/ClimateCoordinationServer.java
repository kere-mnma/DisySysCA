/*
 * SDG 15 - Life on Land
 * Smart Climate and Wildfire Risk Monitoring System
 */
package solutionca.distsysca;

import generated.grpc.climatecoordinationservice.ClimateCoordinationServiceGrpc.ClimateCoordinationServiceImplBase;
import generated.grpc.climatecoordinationservice.CoordinationMessage;
import generated.grpc.climatecoordinationservice.CoordinationResponse;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.LocalTime;
import java.util.logging.Logger;

/**
 * ClimateCoordinationService Server
 *
 * Implements the RPC method defined in climate_coordination_service.proto:
 *   1. coordinateResponse() - BIDIRECTIONAL STREAMING RPC
 *
 * The class extends ClimateCoordinationServiceImplBase which is generated
 * by protoc from climate_coordination_service.proto. Same pattern as
 * ZooService1 extending ZooService1ImplBase from the class sample.
 *
 * BIDIRECTIONAL STREAMING means:
 *   - The CLIENT streams CoordinationMessage updates to the server
 *   - The SERVER streams CoordinationResponse instructions back
 *   - Both happen simultaneously on the same open connection
 */
public class ClimateCoordinationServer extends ClimateCoordinationServiceImplBase {

    private static final Logger logger = Logger.getLogger(ClimateCoordinationServer.class.getName());

    public static void main(String[] args) {

        ClimateCoordinationServer coordinationServer = new ClimateCoordinationServer();

        int port = 50053;

        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(coordinationServer)
                    .build()
                    .start();

            logger.info("Server started, listening on " + port);
            System.out.println("***** ClimateCoordinationService Server started, listening on " + port);

            server.awaitTermination();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * BIDIRECTIONAL STREAMING RPC
     * rpc coordinateResponse (stream CoordinationMessage) returns (stream CoordinationResponse) {}
     *
     * The method returns a StreamObserver<CoordinationMessage> because
     * the server needs to LISTEN to what the client is streaming in.
     * The responseObserver parameter is what the server uses to stream
     * CoordinationResponse messages back to the client simultaneously.
     *
     * For each CoordinationMessage received from an agency client,
     * the server immediately analyses it and streams back a
     * CoordinationResponse with recommended actions, risk level,
     * resource allocation and evacuation instructions.
     *
     * This is the same anonymous StreamObserver pattern as ZooService1
     * averageTemperature() - the server returns a new StreamObserver
     * that handles onNext(), onError() and onCompleted().
     * The difference is that in BIDIRECTIONAL streaming, the server
     * calls responseObserver.onNext() inside onNext() itself -
     * both sides communicate at the same time.
     *
     * @param responseObserver - used to stream responses back to the client
     * @return StreamObserver  - returned to the client to receive incoming messages
     */
    @Override
    public StreamObserver<CoordinationMessage> coordinateResponse(
            StreamObserver<CoordinationResponse> responseObserver) {

        // The server sets up a new observer that handles each CoordinationMessage
        // that arrives from the agency client.
        // Unlike CLIENT STREAMING where we wait for onCompleted() to reply,
        // in BIDIRECTIONAL STREAMING we call responseObserver.onNext() immediately
        // inside onNext() so both sides talk at the same time.
        return new StreamObserver<CoordinationMessage>() {

            @Override
            // When a CoordinationMessage arrives from an agency client,
            // immediately analyse it and stream back a CoordinationResponse
            public void onNext(CoordinationMessage message) {
                System.out.println(LocalTime.now() + ": coordinateResponse() received message"
                        + " | From: "    + message.getSenderOrg()
                        + " | Region: "  + message.getRegionId()
                        + " | Event: "   + message.getEventType()
                        + " | Priority: "+ message.getPriority()
                        + " | Update: "  + message.getStatusUpdate());

                // Compute the wildfire risk level from the incoming message priority
                String riskLevel = computeRiskLevel(message.getPriority());

                // Determine the recommended action based on event type and priority
                String recommendedAction = getRecommendedAction(
                        message.getEventType(), message.getPriority(), message.getRegionId());

                // Determine resource allocation based on priority
                String resourceAllocation = getResourceAllocation(
                        message.getPriority(), message.getRegionId());

                // Determine if evacuation is required
                boolean evacuationRequired = message.getPriority() >= 3
                        && message.getEventType().equalsIgnoreCase("WILDFIRE");

                float evacuationRadiusKm = evacuationRequired
                        ? (message.getPriority() == 4 ? 10.0f : 5.0f) : 0.0f;

                // Build the CoordinationResponse
                CoordinationResponse response = CoordinationResponse.newBuilder()
                        .setResponseId("RESP-" + message.getRegionId()
                                + "-" + System.currentTimeMillis())
                        .setRegionId(message.getRegionId())
                        .setEventType(message.getEventType())
                        .setWildfireRiskLevel(riskLevel)
                        .setRecommendedAction(recommendedAction)
                        .setTargetAgency(message.getSenderOrg())
                        .setResourceAllocation(resourceAllocation)
                        .setPredictedSpread(getPredictedSpread(message.getFireSpreadKm()))
                        .setPriority(message.getPriority())
                        .setEvacuationRequired(evacuationRequired)
                        .setEvacuationRadiusKm(evacuationRadiusKm)
                        .setCoordinationNotes(getCoordinationNotes(message.getPriority()))
                        .setTimestamp(System.currentTimeMillis())
                        .build();

                System.out.println(LocalTime.now() + ": coordinateResponse() streaming response"
                        + " | To: "   + message.getSenderOrg()
                        + " | Risk: " + riskLevel
                        + " | Action: " + recommendedAction);

                // Stream the response back to the client immediately
                // BIDIRECTIONAL: both onNext() calls happen simultaneously
                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println(LocalTime.now()
                        + ": coordinateResponse() error: " + t.getMessage());
            }

            @Override
            // When the client closes their stream, the server closes its stream too
            public void onCompleted() {
                System.out.println(LocalTime.now()
                        + ": coordinateResponse() client stream closed - closing response stream");
                responseObserver.onCompleted();
            }
        };
    }

    // -------------------------------------------------------------------------
    // Helper: Computes risk level from priority number
    // Priority 4=CRITICAL, 3=HIGH, 2=MEDIUM, 1=LOW
    // -------------------------------------------------------------------------
    private String computeRiskLevel(int priority) {
        switch (priority) {
            case 4:  return "CRITICAL";
            case 3:  return "HIGH";
            case 2:  return "MEDIUM";
            default: return "LOW";
        }
    }

    // -------------------------------------------------------------------------
    // Helper: Returns the recommended action based on event type and priority
    // -------------------------------------------------------------------------
    private String getRecommendedAction(String eventType, int priority, String regionId) {
        if (eventType.equalsIgnoreCase("WILDFIRE") && priority == 4) {
            return "IMMEDIATE EVACUATION of " + regionId
                    + ". Deploy all aerial and ground units. Close all access roads.";
        } else if (eventType.equalsIgnoreCase("WILDFIRE") && priority == 3) {
            return "Deploy rapid response fire teams to " + regionId
                    + ". Pre-position evacuation resources. Alert local communities.";
        } else if (eventType.equalsIgnoreCase("DROUGHT") && priority >= 3) {
            return "Activate water conservation protocols in " + regionId
                    + ". Distribute emergency water supplies.";
        } else if (eventType.equalsIgnoreCase("DEFORESTATION")) {
            return "Deploy forest rangers and aerial surveillance to " + regionId
                    + ". Document and report illegal activity.";
        } else {
            return "Increase monitoring frequency in " + regionId
                    + ". Pre-position response teams on standby.";
        }
    }

    // -------------------------------------------------------------------------
    // Helper: Returns resource allocation based on priority
    // -------------------------------------------------------------------------
    private String getResourceAllocation(int priority, String regionId) {
        switch (priority) {
            case 4:  return "2 x aerial water bombers, 4 x ground fire units, "
                           + "3 x evacuation buses - all dispatched to " + regionId;
            case 3:  return "1 x aerial water bomber, 2 x ground fire units dispatched to " + regionId;
            case 2:  return "1 x ground monitoring unit pre-positioned near " + regionId;
            default: return "No additional resources deployed. Monitoring continues.";
        }
    }

    // -------------------------------------------------------------------------
    // Helper: Predicts event spread from fire_spread_km field
    // -------------------------------------------------------------------------
    private String getPredictedSpread(float fireSpreadKm) {
        if (fireSpreadKm > 0) {
            return "Current spread: " + fireSpreadKm + " sq km. "
                    + "Estimated to reach adjacent zone in approximately "
                    + (int) (fireSpreadKm * 4) + " minutes at current rate.";
        }
        return "Spread data unavailable. Await next sensor update.";
    }

    // -------------------------------------------------------------------------
    // Helper: Returns coordination notes based on priority
    // -------------------------------------------------------------------------
    private String getCoordinationNotes(int priority) {
        if (priority == 4) {
            return "ALL AGENCIES: Do not position units NE of fire front. "
                    + "Wind direction unpredictable. Next update in 5 minutes.";
        }
        return "Situation developing. All agencies maintain communication. "
                + "Next coordination update in 10 minutes.";
    }
}