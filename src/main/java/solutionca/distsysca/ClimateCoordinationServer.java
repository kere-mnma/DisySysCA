/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package solutionca.distsysca;

/**
 * @author Eziagbor Osele
 * SDG 15 - Life on Land
 * Project Topic - Smart Climate and Wildfire Risk Monitoring System
 */

import generated.grpc.climatecoordinationservice.ClimateCoordinationServiceGrpc.ClimateCoordinationServiceImplBase;
import generated.grpc.climatecoordinationservice.CoordinationMessage;
import generated.grpc.climatecoordinationservice.CoordinationResponse;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.LocalTime;
import java.util.logging.Logger;

public class ClimateCoordinationServer extends ClimateCoordinationServiceImplBase {

    private static final Logger logger = Logger.getLogger(ClimateCoordinationServer.class.getName());

    static final Metadata.Key<String> CLIENT_ID_KEY =
            Metadata.Key.of("client-id", Metadata.ASCII_STRING_MARSHALLER);

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

            WildfireServiceRegistration reg = WildfireServiceRegistration.getInstance();
            reg.registerService(
                    "_grpc._tcp.local.",
                    "ClimateCoordinationService",
                    port,
                    "SDG15 - Climate Coordination Service: Multi-Agency Disaster Response Hub"
            );

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
     */
    @Override
    public StreamObserver<CoordinationMessage> coordinateResponse(
            StreamObserver<CoordinationResponse> responseObserver) {

        return new StreamObserver<CoordinationMessage>() {

            @Override
            public void onNext(CoordinationMessage message) {

                if (Context.current().isCancelled()) {
                    System.out.println(LocalTime.now()
                            + ": coordinateResponse() - client cancelled. Stopping.");
                    responseObserver.onError(Status.CANCELLED
                            .withDescription("Coordination stream cancelled by client")
                            .asRuntimeException());
                    return;
                }

                System.out.println(LocalTime.now() + ": coordinateResponse() received message"
                        + " | From: "     + message.getSenderOrg()
                        + " | Region: "   + message.getRegionId()
                        + " | Event: "    + message.getEventType()
                        + " | Priority: " + message.getPriority()
                        + " | Update: "   + message.getStatusUpdate());

                if (message.getSenderOrg().isEmpty()) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("sender_org is required in every CoordinationMessage")
                            .asRuntimeException());
                    return;
                }

                if (message.getRegionId().isEmpty()) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("region_id is required in every CoordinationMessage")
                            .asRuntimeException());
                    return;
                }

                if (message.getPriority() < 1 || message.getPriority() > 4) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("priority must be between 1 and 4. Received: "
                                    + message.getPriority())
                            .asRuntimeException());
                    return;
                }

                String riskLevel          = computeRiskLevel(message.getPriority());
                String recommendedAction  = getRecommendedAction(
                        message.getEventType(), message.getPriority(), message.getRegionId());
                String resourceAllocation = getResourceAllocation(
                        message.getPriority(), message.getRegionId());

                boolean evacuationRequired = message.getPriority() >= 3
                        && message.getEventType().equalsIgnoreCase("WILDFIRE");
                float evacuationRadiusKm   = evacuationRequired
                        ? (message.getPriority() == 4 ? 10.0f : 5.0f) : 0.0f;

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

                System.out.println(LocalTime.now() + ": coordinateResponse() response streamed"
                        + " | To: "     + message.getSenderOrg()
                        + " | Risk: "   + riskLevel
                        + " | Action: " + recommendedAction);

                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println(LocalTime.now()
                        + ": coordinateResponse() client stream ERROR: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println(LocalTime.now()
                        + ": coordinateResponse() client stream closed - closing response stream");
                responseObserver.onCompleted();
            }
        };
    }

    private String computeRiskLevel(int priority) {
        switch (priority) {
            case 4:  return "CRITICAL";
            case 3:  return "HIGH";
            case 2:  return "MEDIUM";
            default: return "LOW";
        }
    }

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

    private String getResourceAllocation(int priority, String regionId) {
        switch (priority) {
            case 4:  return "2 x aerial water bombers, 4 x ground fire units, "
                           + "3 x evacuation buses - all dispatched to " + regionId;
            case 3:  return "1 x aerial water bomber, 2 x ground fire units dispatched to " + regionId;
            case 2:  return "1 x ground monitoring unit pre-positioned near " + regionId;
            default: return "No additional resources deployed. Monitoring continues.";
        }
    }

    private String getPredictedSpread(float fireSpreadKm) {
        if (fireSpreadKm > 0) {
            return "Current spread: " + fireSpreadKm + " sq km. Estimated to reach adjacent zone in "
                    + (int) (fireSpreadKm * 4) + " minutes at current rate.";
        }
        return "Spread data unavailable. Await next sensor update.";
    }

    private String getCoordinationNotes(int priority) {
        if (priority == 4) {
            return "ALL AGENCIES: Do not position units NE of fire front. "
                    + "Wind direction unpredictable. Next update in 5 minutes.";
        }
        return "Situation developing. All agencies maintain communication. "
                + "Next coordination update in 10 minutes.";
    }
}