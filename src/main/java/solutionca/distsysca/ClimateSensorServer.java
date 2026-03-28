/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package solutionca.distsysca;

import generated.grpc.climatesensorservice.ClimateSensorServiceGrpc.ClimateSensorServiceImplBase;
import generated.grpc.climatesensorservice.SensorRequest;
import generated.grpc.climatesensorservice.SensorReading;
import generated.grpc.climatesensorservice.SensorSummary;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.logging.Logger;

public class ClimateSensorServer extends ClimateSensorServiceImplBase {

    private static final Logger logger = Logger.getLogger(ClimateSensorServer.class.getName());

    static final Metadata.Key<String> CLIENT_ID_KEY =
            Metadata.Key.of("client-id", Metadata.ASCII_STRING_MARSHALLER);

    public static void main(String[] args) {

        ClimateSensorServer sensorServer = new ClimateSensorServer();
        int port = 50052;

        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(sensorServer)
                    .build()
                    .start();

            logger.info("Server started, listening on " + port);
            System.out.println("***** ClimateSensorService Server started, listening on " + port);

            WildfireServiceRegistration reg = WildfireServiceRegistration.getInstance();
            reg.registerService(
                    "_grpc._tcp.local.",
                    "ClimateSensorService",
                    port,
                    "SDG15 - Climate Sensor Service: Environmental Data Collection and Risk Analysis"
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
     * rpc getLatestReading (SensorRequest) returns (SensorReading) {}
     */
   
    @Override
    public void getLatestReading(SensorRequest request,
            StreamObserver<SensorReading> responseObserver) {

        System.out.println(LocalTime.now() + ": getLatestReading() received"
                + " | Sensor ID: " + request.getSensorId()
                + " | Region: " + request.getRegionId());

        if (request.getSensorId().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("sensor_id is required and cannot be empty")
                    .asRuntimeException());
            return;
        }

        if (!request.getSensorId().toUpperCase().contains("SENSOR")) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Sensor [" + request.getSensorId()
                            + "] not found. Sensor IDs must follow format: SENSOR-XXX")
                    .asRuntimeException());
            return;
        }

        String sensorId = request.getSensorId().toUpperCase();
        int seed = 0;
        String digits = sensorId.replaceAll("[^0-9]", "");
        if (!digits.isEmpty()) {
            seed = Integer.parseInt(digits) % 10; // 0-9 range
        }

        // Compute 
        float temperature;
        float humidity;
        int co2Ppm;
        float windSpeed;
        float soilMoisture;
        String batteryStatus;
        String zone;

        if (seed <= 2) {
            // Critical wildfire conditions
            temperature = 44.0f + seed * 1.5f;    
            humidity = 9.5f - seed * 0.8f;    
            co2Ppm = 800 + seed * 40;       
            windSpeed = 46.0f + seed * 3.0f;     
            soilMoisture = 10.0f - seed * 0.5f;     
            batteryStatus = "GOOD";
            zone = "DANGER ZONE";
        } else if (seed <= 4) {
            // High risk conditions
            temperature = 38.0f + seed * 0.8f;    
            humidity = 18.0f - seed * 1.0f;    
            co2Ppm = 600 + seed * 20;      
            windSpeed = 35.0f + seed * 1.5f;     
            soilMoisture = 14.0f - seed * 0.5f;     
            batteryStatus = "FULL";
            zone = "HIGH RISK ZONE";
        } else if (seed <= 6) {
            // Moderate conditions
            temperature = 33.0f + seed * 0.5f;    
            humidity = 24.0f + seed * 0.5f;   
            co2Ppm = 480 + seed * 10;       
            windSpeed = 25.0f + seed * 0.8f;     
            soilMoisture = 17.0f + seed * 0.3f;    
            batteryStatus = "FULL";
            zone = "MODERATE ZONE";
        } else {
            // Normal safe conditions
            temperature = 26.0f + seed * 0.4f;    
            humidity = 42.0f + seed * 1.5f;    
            co2Ppm = 410 + seed * 3;        
            windSpeed = 10.0f + seed * 0.5f;     
            soilMoisture = 32.0f + seed * 1.2f;     
            batteryStatus = "FULL";
            zone = "NORMAL ZONE";
        }

        // GPS coordinates 
        float latitude = 6.5244f + (seed * 0.05f);
        float longitude = 3.3792f + (seed * 0.04f);

        SensorReading reading = SensorReading.newBuilder()
                .setSensorId(request.getSensorId())
                .setRegionId(request.getRegionId())
                .setTemperature(temperature)
                .setHumidity(humidity)
                .setCo2Ppm(co2Ppm)
                .setWindSpeed(windSpeed)
                .setSoilMoisture(soilMoisture)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setTimestamp(System.currentTimeMillis())
                .setBatteryStatus(batteryStatus)
                .build();

        System.out.println(LocalTime.now() + ": getLatestReading() response sent"
                + " | Sensor: " + sensorId
                + " | Zone: " + zone
                + " | Temp: " + temperature + "C"
                + " | Humidity: " + humidity + "%"
                + " | CO2: " + co2Ppm + "ppm");

        responseObserver.onNext(reading);
        responseObserver.onCompleted();
    }

    /**
     * CLIENT STREAMING RPC
     * rpc streamSensorReadings (stream SensorReading) returns (SensorSummary) {}
     */
    @Override
    public StreamObserver<SensorReading> streamSensorReadings(
            StreamObserver<SensorSummary> responseObserver) {

        return new StreamObserver<SensorReading>() {

            ArrayList<SensorReading> readings      = new ArrayList<>();
            float totalTemperature = 0;
            float maxTemperature   = 0;
            float totalHumidity    = 0;
            float minHumidity      = 100;
            float totalWindSpeed   = 0;
            float totalCo2         = 0;
            int   readingsRejected = 0;
            ArrayList<String> flaggedSensors = new ArrayList<>();

            @Override
            public void onNext(SensorReading reading) {

                if (Context.current().isCancelled()) {
                    System.out.println(LocalTime.now()
                            + ": streamSensorReadings() - client cancelled. Stopping.");
                    responseObserver.onError(Status.CANCELLED
                            .withDescription("Client cancelled the sensor reading stream")
                            .asRuntimeException());
                    return;
                }

                System.out.println(LocalTime.now() + ": streamSensorReadings() received reading #"
                        + (readings.size() + 1)
                        + " | Sensor: "   + reading.getSensorId()
                        + " | Temp: "     + reading.getTemperature() + "C"
                        + " | Humidity: " + reading.getHumidity() + "%");

                if (reading.getSensorId().isEmpty()) {
                    readingsRejected++;
                    System.out.println(LocalTime.now()
                            + ": streamSensorReadings() REJECTED reading - missing sensor_id");
                    return;
                }

                readings.add(reading);
                totalTemperature += reading.getTemperature();
                totalHumidity    += reading.getHumidity();
                totalWindSpeed   += reading.getWindSpeed();
                totalCo2         += reading.getCo2Ppm();

                if (reading.getTemperature() > maxTemperature) maxTemperature = reading.getTemperature();
                if (reading.getHumidity()    < minHumidity)    minHumidity    = reading.getHumidity();

                if (reading.getTemperature() > 42.0f || reading.getHumidity() < 10.0f) {
                    if (!flaggedSensors.contains(reading.getSensorId())) {
                        flaggedSensors.add(reading.getSensorId());
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                System.out.println(LocalTime.now()
                        + ": streamSensorReadings() client stream ERROR: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println(LocalTime.now() + ": streamSensorReadings() stream complete"
                        + " | Saved: " + readings.size()
                        + " | Rejected: " + readingsRejected);

                if (readings.isEmpty()) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("All " + readingsRejected
                                    + " readings were rejected. "
                                    + "Each reading must include a valid sensor_id.")
                            .asRuntimeException());
                    return;
                }

                int saved = readings.size();

                float avgTemp     = totalTemperature / saved;
                float avgHumidity = totalHumidity    / saved;
                float avgWind     = totalWindSpeed   / saved;
                float avgCo2      = totalCo2         / saved;

                String riskAssessment;
                String riskReasoning;

                if (avgTemp > 42.0f && avgHumidity < 10.0f) {
                    riskAssessment = "CRITICAL - IMMEDIATE ACTION REQUIRED";
                    riskReasoning  = "Avg temp " + avgTemp + "C + avg humidity "
                            + avgHumidity + "% + wind " + avgWind + "km/h = CRITICAL";
                } else if (avgTemp > 38.0f && avgHumidity < 20.0f) {
                    riskAssessment = "HIGH RISK";
                    riskReasoning  = "Avg temp " + avgTemp + "C + avg humidity "
                            + avgHumidity + "% = HIGH danger of wildfire ignition";
                } else if (avgTemp > 33.0f && avgHumidity < 30.0f) {
                    riskAssessment = "MODERATE RISK";
                    riskReasoning  = "Conditions developing. Monitor closely. Avg temp " + avgTemp + "C";
                } else {
                    riskAssessment = "LOW RISK";
                    riskReasoning  = "Conditions within normal range. Avg temp " + avgTemp + "C";
                }

                SensorSummary summary = SensorSummary.newBuilder()
                        .setStatus(readingsRejected == 0 ? "SUCCESS" : "PARTIAL_FAILURE")
                        .setReadingsReceived(saved + readingsRejected)
                        .setReadingsSaved(saved)
                        .setReadingsRejected(readingsRejected)
                        .setAvgTemperature(avgTemp)
                        .setMaxTemperature(maxTemperature)
                        .setAvgHumidity(avgHumidity)
                        .setMinHumidity(minHumidity)
                        .setAvgWindSpeed(avgWind)
                        .setAvgCo2Ppm(avgCo2)
                        .setRiskAssessment(riskAssessment)
                        .setRiskReasoning(riskReasoning)
                        .addAllFlaggedSensors(flaggedSensors)
                        .setProcessedAt(System.currentTimeMillis())
                        .build();

                System.out.println(LocalTime.now() + ": streamSensorReadings() summary sent"
                        + " | Risk: " + riskAssessment
                        + " | Flagged: " + flaggedSensors.size());

                responseObserver.onNext(summary);
                responseObserver.onCompleted();
            }
        };
    }
}