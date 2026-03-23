/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package solutionca.distsysca;

/**
 * SDG 15 - Life on Land
 * Smart Climate and Wildfire Risk Monitoring System
 */

import generated.grpc.alertservice.AlertServiceGrpc.AlertServiceImplBase;
import generated.grpc.alertservice.IncidentReport;
import generated.grpc.alertservice.IncidentAcknowledgement;
import generated.grpc.alertservice.AlertSubscription;
import generated.grpc.alertservice.AlertNotification;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.LocalTime;
import java.util.logging.Logger;

public class AlertServer extends AlertServiceImplBase {

    private static final Logger logger = Logger.getLogger(AlertServer.class.getName());

    static final Metadata.Key<String> CLIENT_ID_KEY =
            Metadata.Key.of("client-id", Metadata.ASCII_STRING_MARSHALLER);

    public static void main(String[] args) {

        AlertServer alertServer = new AlertServer();
        int port = 50051;

        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(alertServer)
                    .build()
                    .start();

            logger.info("Server started, listening on " + port);
            System.out.println("***** AlertService Server started, listening on " + port);

            WildfireServiceRegistration reg = WildfireServiceRegistration.getInstance();
            reg.registerService(
                    "_grpc._tcp.local.",
                    "AlertService",
                    port,
                    "SDG15 - Alert Service: Wildfire and Climate Incident Reporting"
            );

            server.awaitTermination();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * UNARY RPC
     * rpc reportIncident (IncidentReport) returns (IncidentAcknowledgement) {}
     */
    @Override
    public void reportIncident(IncidentReport request,
            StreamObserver<IncidentAcknowledgement> responseObserver) {

        System.out.println(LocalTime.now() + ": reportIncident() received"
                + " | Region: "   + request.getRegionId()
                + " | Event: "    + request.getEventType()
                + " | Severity: " + request.getSeverity()
                + " | Reporter: " + request.getReporterId());

        if (request.getRegionId().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("region_id is required and cannot be empty")
                    .asRuntimeException());
            return;
        }

        if (request.getEventType().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("event_type is required and cannot be empty")
                    .asRuntimeException());
            return;
        }

        if (request.getSeverity().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("severity is required: LOW, MEDIUM, HIGH or CRITICAL")
                    .asRuntimeException());
            return;
        }

        String sev = request.getSeverity().toUpperCase();
        if (!sev.equals("LOW") && !sev.equals("MEDIUM")
                && !sev.equals("HIGH") && !sev.equals("CRITICAL")) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Invalid severity: [" + request.getSeverity()
                            + "]. Accepted values: LOW, MEDIUM, HIGH, CRITICAL")
                    .asRuntimeException());
            return;
        }

        // Incident ID is generated
        String incidentId = "INC-"
                + request.getRegionId().replace("-", "").toUpperCase()
                + "-" + System.currentTimeMillis();

        String status;
        String assignedTeam;
        String estimatedArrival;

        if (sev.equals("CRITICAL")) {
            status           = "ESCALATED";
            assignedTeam     = "National Emergency Response Unit - " + request.getRegionId();
            estimatedArrival = "8 minutes - PRIORITY DISPATCH";
        } else if (sev.equals("HIGH")) {
            status           = "LOGGED";
            assignedTeam     = "Regional Rapid Response Team - " + request.getRegionId();
            estimatedArrival = "15 minutes";
        } else {
            status           = "LOGGED";
            assignedTeam     = "Local Monitoring Unit - " + request.getRegionId();
            estimatedArrival = "30 minutes";
        }

        IncidentAcknowledgement acknowledgement = IncidentAcknowledgement.newBuilder()
                .setIncidentId(incidentId)
                .setStatus(status)
                .setAssignedTeam(assignedTeam)
                .setEstimatedArrival(estimatedArrival)
                .setMessage(request.getEventType() + " [" + sev + "] logged in "
                        + request.getRegionId() + ". " + assignedTeam + " dispatched.")
                .setLoggedAt(System.currentTimeMillis())
                .build();

        System.out.println(LocalTime.now() + ": reportIncident() response sent"
                + " | Incident ID: " + incidentId + " | Status: " + status);

        responseObserver.onNext(acknowledgement);
        responseObserver.onCompleted();
    }

    /*
     * SERVER STREAMING RPC
     * rpc subscribeToAlerts (AlertSubscription) returns (stream AlertNotification) {}
     */
    @Override
    public void subscribeToAlerts(AlertSubscription request,
            StreamObserver<AlertNotification> responseObserver) {

        System.out.println(LocalTime.now() + ": subscribeToAlerts() received"
                + " | Subscriber: "     + request.getSubscriberId()
                + " | Region: "         + request.getRegionId()
                + " | Min Risk Level: " + request.getMinRiskLevel());

        if (request.getRegionId().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("region_id is required to subscribe to alerts")
                    .asRuntimeException());
            return;
        }

        if (request.getSubscriberId().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("subscriber_id is required to subscribe to alerts")
                    .asRuntimeException());
            return;
        }

        // Simulated environmental alerts
        String[][] alerts = {
            {"WILDFIRE",      "HIGH",     "47.5", "9.2",  "35.0",
             "Temperature rising rapidly. Dry vegetation detected in northern sector."},
            {"WILDFIRE",      "HIGH",     "49.1", "6.3",  "52.0",
             "Fire front advancing. Wind speed increasing to 52km/h. Road access limited."},
            {"HEATWAVE",      "MEDIUM",   "43.0", "14.5", "28.0",
             "Sustained high temperatures. Soil moisture critically low across Zone B."},
            {"WILDFIRE",      "CRITICAL", "51.3", "4.1",  "61.0",
             "CRITICAL: Fire crossed highway. Immediate evacuation recommended."},
            {"DEFORESTATION", "MEDIUM",   "38.0", "22.0", "18.0",
             "Unusual vegetation loss detected in northern sector. Rangers alerted."}
        };

        for (String[] alert : alerts) {

            if (Context.current().isCancelled()) {
                System.out.println(LocalTime.now()
                        + ": subscribeToAlerts() - client cancelled stream. Stopping.");
                responseObserver.onError(Status.CANCELLED
                        .withDescription("Stream cancelled by client")
                        .asRuntimeException());
                return;
            }

            AlertNotification notification = AlertNotification.newBuilder()
                    .setAlertId("ALERT-" + request.getRegionId() + "-" + System.currentTimeMillis())
                    .setRegionId(request.getRegionId())
                    .setEventType(alert[0])
                    .setRiskLevel(alert[1])
                    .setTemperature(Float.parseFloat(alert[2]))
                    .setHumidity(Float.parseFloat(alert[3]))
                    .setWindSpeed(Float.parseFloat(alert[4]))
                    .setDescription(alert[5])
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            System.out.println(LocalTime.now() + ": subscribeToAlerts() streaming alert"
                    + " | Event: "    + alert[0]
                    + " | Risk: "     + alert[1]
                    + " | Temp: "     + alert[2] + "C"
                    + " | Humidity: " + alert[3] + "%");

            responseObserver.onNext(notification);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        responseObserver.onCompleted();
        System.out.println(LocalTime.now() + ": subscribeToAlerts() stream completed for: "
                + request.getSubscriberId());
    }
}