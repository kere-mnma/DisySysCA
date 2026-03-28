/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package solutionca.distsysca;


import generated.grpc.alertservice.AlertServiceGrpc;
import generated.grpc.alertservice.AlertServiceGrpc.AlertServiceBlockingStub;
import generated.grpc.alertservice.AlertServiceGrpc.AlertServiceStub;
import generated.grpc.alertservice.IncidentReport;
import generated.grpc.alertservice.IncidentAcknowledgement;
import generated.grpc.alertservice.AlertSubscription;
import generated.grpc.alertservice.AlertNotification;

import generated.grpc.climatesensorservice.ClimateSensorServiceGrpc;
import generated.grpc.climatesensorservice.ClimateSensorServiceGrpc.ClimateSensorServiceBlockingStub;
import generated.grpc.climatesensorservice.ClimateSensorServiceGrpc.ClimateSensorServiceStub;
import generated.grpc.climatesensorservice.SensorRequest;
import generated.grpc.climatesensorservice.SensorReading;
import generated.grpc.climatesensorservice.SensorSummary;

import generated.grpc.climatecoordinationservice.ClimateCoordinationServiceGrpc;
import generated.grpc.climatecoordinationservice.ClimateCoordinationServiceGrpc.ClimateCoordinationServiceStub;
import generated.grpc.climatecoordinationservice.CoordinationMessage;
import generated.grpc.climatecoordinationservice.CoordinationResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import javax.jmdns.ServiceInfo;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WildfireMonitorGUI extends JFrame {

    // gRPC channels
    private ManagedChannel alertChannel;
    private ManagedChannel sensorChannel;
    private ManagedChannel coordinationChannel;

    // gRPC stubs
    private AlertServiceBlockingStub         alertBlockingStub;
    private AlertServiceStub                 alertAsyncStub;
    private ClimateSensorServiceBlockingStub sensorBlockingStub;
    private ClimateSensorServiceStub         sensorAsyncStub;
    private ClimateCoordinationServiceStub   coordinationAsyncStub;

    // METADATA - client-id header key

    private static final Metadata.Key<String> CLIENT_ID_KEY =
            Metadata.Key.of("client-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final String CLIENT_ID = "WildfireMonitorGUI-NCI-001";


    // Output 

    private JTextArea alertOutputArea;
    private JTextArea sensorOutputArea;
    private JTextArea coordinationOutputArea;

    // Status labels

    private JLabel alertStatusLabel;
    private JLabel sensorStatusLabel;
    private JLabel coordinationStatusLabel;

    // Bidirectional stream observer 

    private StreamObserver<CoordinationMessage> coordinationRequestObserver;

    // CANCELLATION

    private Thread activeAlertStreamThread = null;
    private volatile boolean alertStreamCancelled = false;

    
    // Pending sensor readings list for client streaming

    private final List<SensorReading>      pendingReadings  = new ArrayList<>();
    private final DefaultListModel<String> pendingListModel = new DefaultListModel<>();

    // Constructor

    public WildfireMonitorGUI() {
        setTitle("SDG 15 - Wildfire & Climate Risk Monitoring System");
        setSize(900, 700);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        JTabbedPane tabbedPane = new JTabbedPane();
        tabbedPane.addTab("Alert Service",  buildAlertTab());
        tabbedPane.addTab("Climate Sensor", buildSensorTab());
        tabbedPane.addTab("Coordination",   buildCoordinationTab());
        add(tabbedPane, BorderLayout.CENTER);

        JPanel statusBar = new JPanel(new GridLayout(1, 3));
        alertStatusLabel        = new JLabel("AlertService: Searching...",        SwingConstants.CENTER);
        sensorStatusLabel       = new JLabel("SensorService: Searching...",       SwingConstants.CENTER);
        coordinationStatusLabel = new JLabel("CoordinationService: Searching...", SwingConstants.CENTER);
        alertStatusLabel.setForeground(Color.RED);
        sensorStatusLabel.setForeground(Color.RED);
        coordinationStatusLabel.setForeground(Color.RED);
        statusBar.add(alertStatusLabel);
        statusBar.add(sensorStatusLabel);
        statusBar.add(coordinationStatusLabel);
        add(statusBar, BorderLayout.SOUTH);

        new Thread(() -> {
            try {
                Thread.sleep(3000); // wait 3 seconds for servers to register with jmDNS
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            discoverAllServices();
        }).start();
    }

    private void discoverAllServices() {
        discoverAndConnect("AlertService",               50051, "alertService");
        discoverAndConnect("ClimateSensorService",       50052, "sensorService");
        discoverAndConnect("ClimateCoordinationService", 50053, "coordinationService");
    }

    private void discoverAndConnect(String serviceName, int fallbackPort, String serviceKey) {
        try {
            System.out.println("Discovering: " + serviceName + "...");
            WildfireServiceDiscovery discovery = new WildfireServiceDiscovery(
                    "_grpc._tcp.local.", serviceName);
            ServiceInfo info = discovery.discoverService(15000);

            int    port = (info != null) ? info.getPort() : fallbackPort;
            String host = "localhost";

            Metadata headers = new Metadata();
            headers.put(CLIENT_ID_KEY, CLIENT_ID);

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
                    .build();

            SwingUtilities.invokeLater(() -> {
                switch (serviceKey) {
                    case "alertService":
                        alertChannel      = channel;
                        alertBlockingStub = AlertServiceGrpc.newBlockingStub(channel);
                        alertAsyncStub    = AlertServiceGrpc.newStub(channel);
                        alertStatusLabel.setText("AlertService: Connected :" + port);
                        alertStatusLabel.setForeground(new Color(0, 128, 0));
                        break;
                    case "sensorService":
                        sensorChannel      = channel;
                        sensorBlockingStub = ClimateSensorServiceGrpc.newBlockingStub(channel);
                        sensorAsyncStub    = ClimateSensorServiceGrpc.newStub(channel);
                        sensorStatusLabel.setText("SensorService: Connected :" + port);
                        sensorStatusLabel.setForeground(new Color(0, 128, 0));
                        break;
                    case "coordinationService":
                        coordinationChannel   = channel;
                        coordinationAsyncStub = ClimateCoordinationServiceGrpc.newStub(channel);
                        coordinationStatusLabel.setText("CoordinationService: Connected :" + port);
                        coordinationStatusLabel.setForeground(new Color(0, 128, 0));
                        break;
                }
            });

            discovery.close();

        } catch (Exception e) {
            System.out.println("Discovery failed for " + serviceName + ": " + e.getMessage());
        }
    }


    // AlertService
 
    private JPanel buildAlertTab() {
        JPanel panel = new JPanel(new BorderLayout(5, 5));

        JPanel inputPanel = new JPanel(new GridLayout(6, 2, 5, 5));
        inputPanel.setBorder(BorderFactory.createTitledBorder("Alert Service Inputs"));

        JTextField regionField    = new JTextField("ZONE-A-LAGOS");
        JTextField eventField     = new JTextField("WILDFIRE");
        JTextField severityField  = new JTextField("HIGH");
        JTextField reporterField  = new JTextField("SENSOR-001");
        JTextField subRegionField = new JTextField("ZONE-A-LAGOS");
        JTextField riskLevelField = new JTextField("HIGH");

        inputPanel.add(new JLabel("Region ID (Incident):"));      inputPanel.add(regionField);
        inputPanel.add(new JLabel("Event Type:"));                inputPanel.add(eventField);
        inputPanel.add(new JLabel("Severity (LOW/MED/HIGH/CRIT):")); inputPanel.add(severityField);
        inputPanel.add(new JLabel("Reporter ID:"));               inputPanel.add(reporterField);
        inputPanel.add(new JLabel("Subscribe Region:"));          inputPanel.add(subRegionField);
        inputPanel.add(new JLabel("Min Risk Level (Subscribe):")); inputPanel.add(riskLevelField);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton reportBtn    = new JButton("Report Incident (Unary)");
        JButton subscribeBtn = new JButton("Subscribe to Alerts (Server Streaming)");
        JButton cancelBtn    = new JButton("Cancel Stream");
        JButton clearBtn     = new JButton("Clear Output");
        cancelBtn.setEnabled(false);
        buttonPanel.add(reportBtn);
        buttonPanel.add(subscribeBtn);
        buttonPanel.add(cancelBtn);
        buttonPanel.add(clearBtn);

        alertOutputArea = new JTextArea();
        alertOutputArea.setEditable(false);
        alertOutputArea.setFont(new Font("Monospaced", Font.PLAIN, 12));

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(inputPanel,  BorderLayout.CENTER);
        topPanel.add(buttonPanel, BorderLayout.SOUTH);
        panel.add(topPanel,                         BorderLayout.NORTH);
        panel.add(new JScrollPane(alertOutputArea), BorderLayout.CENTER);

        reportBtn.addActionListener((ActionEvent e) -> {
            if (alertBlockingStub == null) {
                alertOutputArea.append("ERROR: AlertService not connected yet.\n"); return;
            }
            new Thread(() -> {
                try {
                    IncidentReport request = IncidentReport.newBuilder()
                            .setRegionId(regionField.getText().trim())
                            .setEventType(eventField.getText().trim())
                            .setSeverity(severityField.getText().trim())
                            .setReporterId(reporterField.getText().trim())
                            .setTemperature(47.5f)
                            .setHumidity(8.2f)
                            .setTimestamp(System.currentTimeMillis())
                            .build();

                    // DEADLINE: if server does not respond in 10 seconds, fail with DEADLINE_EXCEEDED
                    IncidentAcknowledgement response = alertBlockingStub
                            .withDeadlineAfter(10, TimeUnit.SECONDS)
                            .reportIncident(request);

                    SwingUtilities.invokeLater(() -> {
                        alertOutputArea.append("=== reportIncident() UNARY RESPONSE ===\n");
                        alertOutputArea.append("Incident ID:       " + response.getIncidentId()       + "\n");
                        alertOutputArea.append("Status:            " + response.getStatus()            + "\n");
                        alertOutputArea.append("Assigned Team:     " + response.getAssignedTeam()     + "\n");
                        alertOutputArea.append("Estimated Arrival: " + response.getEstimatedArrival() + "\n");
                        alertOutputArea.append("Message:           " + response.getMessage()           + "\n");
                        alertOutputArea.append("=======================================\n");
                    });

                } catch (StatusRuntimeException ex) {
                    // ERROR HANDLING: display the gRPC status code and description
                    SwingUtilities.invokeLater(() ->
                            alertOutputArea.append("gRPC ERROR [" + ex.getStatus().getCode()
                                    + "]: " + ex.getStatus().getDescription() + "\n"));
                }
            }).start();
        });

        subscribeBtn.addActionListener((ActionEvent e) -> {
            if (alertBlockingStub == null) {
                alertOutputArea.append("ERROR: AlertService not connected yet.\n"); return;
            }

            alertStreamCancelled = false;
            subscribeBtn.setEnabled(false);
            cancelBtn.setEnabled(true);

            activeAlertStreamThread = new Thread(() -> {
                try {
                    AlertSubscription subscription = AlertSubscription.newBuilder()
                            .setSubscriberId("DASHBOARD-NCI-001")
                            .setRegionId(subRegionField.getText().trim())
                            .setMinRiskLevel(riskLevelField.getText().trim())
                            .build();

                    // DEADLINE: 60 seconds for the full server streaming call
                    Iterator<AlertNotification> alerts = alertBlockingStub
                            .withDeadlineAfter(60, TimeUnit.SECONDS)
                            .subscribeToAlerts(subscription);

                    SwingUtilities.invokeLater(() ->
                            alertOutputArea.append("=== subscribeToAlerts() SERVER STREAMING ===\n"));

                    while (alerts.hasNext() && !alertStreamCancelled) {
                        AlertNotification alert = alerts.next();
                        SwingUtilities.invokeLater(() -> {
                            alertOutputArea.append("[" + alert.getRiskLevel() + "] "
                                    + alert.getEventType() + " - " + alert.getRegionId() + "\n");
                            alertOutputArea.append("  Temp: "     + alert.getTemperature() + "C"
                                    + " | Humidity: " + alert.getHumidity()  + "%"
                                    + " | Wind: "     + alert.getWindSpeed() + "km/h\n");
                            alertOutputArea.append("  " + alert.getDescription() + "\n");
                            alertOutputArea.append("-------------------------------------------\n");
                        });
                    }

                    SwingUtilities.invokeLater(() -> {
                        alertOutputArea.append(alertStreamCancelled
                                ? "=== Stream cancelled by user ===\n"
                                : "=== Stream ended ===\n");
                        subscribeBtn.setEnabled(true);
                        cancelBtn.setEnabled(false);
                    });

                } catch (StatusRuntimeException ex) {
                    SwingUtilities.invokeLater(() -> {
                        alertOutputArea.append("gRPC ERROR [" + ex.getStatus().getCode()
                                + "]: " + ex.getStatus().getDescription() + "\n");
                        subscribeBtn.setEnabled(true);
                        cancelBtn.setEnabled(false);
                    });
                }
            });
            activeAlertStreamThread.start();
        });

        cancelBtn.addActionListener((ActionEvent e) -> {
            alertStreamCancelled = true;
            if (activeAlertStreamThread != null) {
                activeAlertStreamThread.interrupt();
            }
            alertOutputArea.append("=== Cancellation requested ===\n");
            cancelBtn.setEnabled(false);
        });

        clearBtn.addActionListener(e -> alertOutputArea.setText(""));

        return panel;
    }

    // ClimateSensorService
    private JPanel buildSensorTab() {
        JPanel panel = new JPanel(new BorderLayout(5, 5));

        // Unary section
        JPanel unaryPanel = new JPanel(new GridLayout(2, 2, 5, 5));
        unaryPanel.setBorder(BorderFactory.createTitledBorder("Get Latest Reading (Unary)"));
        JTextField sensorIdField    = new JTextField("SENSOR-001");
        JTextField unaryRegionField = new JTextField("ZONE-A-LAGOS");
        unaryPanel.add(new JLabel("Sensor ID:")); unaryPanel.add(sensorIdField);
        unaryPanel.add(new JLabel("Region ID:")); unaryPanel.add(unaryRegionField);
        JButton latestBtn = new JButton("Get Latest Reading");

        // Client streaming section
        JPanel streamInputPanel = new JPanel(new GridLayout(7, 2, 5, 5));
        streamInputPanel.setBorder(BorderFactory.createTitledBorder(
                "Add Sensor Reading to Queue (Client Streaming)"));

        JTextField streamSensorIdField = new JTextField("SENSOR-001");
        JTextField streamRegionField   = new JTextField("ZONE-A-LAGOS");
        JTextField tempField           = new JTextField("44.7");
        JTextField humidityField       = new JTextField("8.3");
        JTextField co2Field            = new JTextField("820");
        JTextField windField           = new JTextField("48.5");
        JTextField soilField           = new JTextField("9.1");

        streamInputPanel.add(new JLabel("Sensor ID:"));          streamInputPanel.add(streamSensorIdField);
        streamInputPanel.add(new JLabel("Region ID:"));          streamInputPanel.add(streamRegionField);
        streamInputPanel.add(new JLabel("Temperature (C):"));    streamInputPanel.add(tempField);
        streamInputPanel.add(new JLabel("Humidity (%):"));       streamInputPanel.add(humidityField);
        streamInputPanel.add(new JLabel("CO2 (ppm):"));          streamInputPanel.add(co2Field);
        streamInputPanel.add(new JLabel("Wind Speed (km/h):"));  streamInputPanel.add(windField);
        streamInputPanel.add(new JLabel("Soil Moisture (%):")); streamInputPanel.add(soilField);

        JList<String> pendingList = new JList<>(pendingListModel);
        pendingList.setFont(new Font("Monospaced", Font.PLAIN, 11));
        JScrollPane queueScroll = new JScrollPane(pendingList);
        queueScroll.setBorder(BorderFactory.createTitledBorder("Readings Queued"));
        queueScroll.setPreferredSize(new Dimension(600, 90));

        JButton addReadingBtn = new JButton("Add Reading to Queue");
        JButton clearQueueBtn = new JButton("Clear Queue");
        JButton sendAllBtn    = new JButton("Send All to Server");
        sendAllBtn.setEnabled(false);

        sensorOutputArea = new JTextArea();
        sensorOutputArea.setEditable(false);
        sensorOutputArea.setFont(new Font("Monospaced", Font.PLAIN, 12));

        JPanel unaryBtnRow  = new JPanel(new FlowLayout(FlowLayout.LEFT));
        unaryBtnRow.add(latestBtn);

        JPanel streamBtnRow = new JPanel(new FlowLayout(FlowLayout.LEFT));
        streamBtnRow.add(addReadingBtn);
        streamBtnRow.add(clearQueueBtn);
        streamBtnRow.add(sendAllBtn);

        JPanel leftCol = new JPanel(new BorderLayout(3, 3));
        leftCol.add(unaryPanel,  BorderLayout.CENTER);
        leftCol.add(unaryBtnRow, BorderLayout.SOUTH);

        JPanel rightCol = new JPanel(new BorderLayout(3, 3));
        rightCol.add(streamInputPanel, BorderLayout.CENTER);
        rightCol.add(streamBtnRow,     BorderLayout.SOUTH);

        JPanel inputsRow = new JPanel(new GridLayout(1, 2, 8, 0));
        inputsRow.add(leftCol);
        inputsRow.add(rightCol);

        JPanel topSection = new JPanel(new BorderLayout(3, 3));
        topSection.add(inputsRow,   BorderLayout.CENTER);
        topSection.add(queueScroll, BorderLayout.SOUTH);

        JButton clearOutputBtn = new JButton("Clear Output");
        JPanel  clearRow       = new JPanel(new FlowLayout(FlowLayout.LEFT));
        clearRow.add(clearOutputBtn);

        panel.add(topSection,                        BorderLayout.NORTH);
        panel.add(new JScrollPane(sensorOutputArea), BorderLayout.CENTER);
        panel.add(clearRow,                          BorderLayout.SOUTH);

        latestBtn.addActionListener((ActionEvent e) -> {
            if (sensorBlockingStub == null) {
                sensorOutputArea.append("ERROR: ClimateSensorService not connected yet.\n"); return;
            }
            new Thread(() -> {
                try {
                    SensorRequest request = SensorRequest.newBuilder()
                            .setSensorId(sensorIdField.getText().trim())
                            .setRegionId(unaryRegionField.getText().trim())
                            .build();

                    // DEADLINE: if server does not respond in 10 seconds, fail
                    SensorReading reading = sensorBlockingStub
                            .withDeadlineAfter(10, TimeUnit.SECONDS)
                            .getLatestReading(request);

                    SwingUtilities.invokeLater(() -> {
                        sensorOutputArea.append("=== getLatestReading() UNARY RESPONSE ===\n");
                        sensorOutputArea.append("Sensor ID:     " + reading.getSensorId()    + "\n");
                        sensorOutputArea.append("Region:        " + reading.getRegionId()     + "\n");
                        sensorOutputArea.append("Temperature:   " + reading.getTemperature()  + "C\n");
                        sensorOutputArea.append("Humidity:      " + reading.getHumidity()     + "%\n");
                        sensorOutputArea.append("CO2:           " + reading.getCo2Ppm()       + " ppm\n");
                        sensorOutputArea.append("Wind Speed:    " + reading.getWindSpeed()    + " km/h\n");
                        sensorOutputArea.append("Soil Moisture: " + reading.getSoilMoisture() + "%\n");
                        sensorOutputArea.append("Battery:       " + reading.getBatteryStatus()+ "\n");
                        sensorOutputArea.append("=========================================\n");
                    });

                } catch (StatusRuntimeException ex) {
                    SwingUtilities.invokeLater(() ->
                            sensorOutputArea.append("gRPC ERROR [" + ex.getStatus().getCode()
                                    + "]: " + ex.getStatus().getDescription() + "\n"));
                }
            }).start();
        });

        // Add Reading to Queue
        addReadingBtn.addActionListener((ActionEvent e) -> {
            try {
                if (streamSensorIdField.getText().trim().isEmpty()) {
                    sensorOutputArea.append("ERROR: Sensor ID is required.\n"); return;
                }
                String sensorId    = streamSensorIdField.getText().trim();
                String regionId    = streamRegionField.getText().trim();
                float  temperature = Float.parseFloat(tempField.getText().trim());
                float  humidity    = Float.parseFloat(humidityField.getText().trim());
                int    co2Ppm      = Integer.parseInt(co2Field.getText().trim());
                float  windSpeed   = Float.parseFloat(windField.getText().trim());
                float  soilMoist   = Float.parseFloat(soilField.getText().trim());

                SensorReading reading = SensorReading.newBuilder()
                        .setSensorId(sensorId).setRegionId(regionId)
                        .setTemperature(temperature).setHumidity(humidity)
                        .setCo2Ppm(co2Ppm).setWindSpeed(windSpeed)
                        .setSoilMoisture(soilMoist)
                        .setTimestamp(System.currentTimeMillis()).build();

                pendingReadings.add(reading);
                pendingListModel.addElement(String.format(
                        "[%d] %s | %s | Temp:%.1fC | Hum:%.1f%% | CO2:%d | Wind:%.1f | Soil:%.1f%%",
                        pendingReadings.size(), sensorId, regionId,
                        temperature, humidity, co2Ppm, windSpeed, soilMoist));
                sendAllBtn.setEnabled(true);
                sensorOutputArea.append("Reading #" + pendingReadings.size() + " queued: "
                        + sensorId + " | Temp: " + temperature + "C | Humidity: " + humidity + "%\n");

                try {
                    if (sensorId.matches(".*\\d+$")) {
                        int num = Integer.parseInt(sensorId.replaceAll("\\D+", ""));
                        streamSensorIdField.setText(
                                sensorId.replaceAll("\\d+$", String.format("%03d", num + 1)));
                    }
                } catch (Exception ignored) {}

            } catch (NumberFormatException ex) {
                sensorOutputArea.append("ERROR: Invalid number in input fields.\n");
            }
        });

        clearQueueBtn.addActionListener((ActionEvent e) -> {
            pendingReadings.clear(); pendingListModel.clear();
            sendAllBtn.setEnabled(false);
            sensorOutputArea.append("Queue cleared.\n");
        });

        sendAllBtn.addActionListener((ActionEvent e) -> {
            if (sensorAsyncStub == null) {
                sensorOutputArea.append("ERROR: ClimateSensorService not connected yet.\n"); return;
            }
            if (pendingReadings.isEmpty()) {
                sensorOutputArea.append("ERROR: Queue is empty.\n"); return;
            }

            List<SensorReading> toSend = new ArrayList<>(pendingReadings);
            pendingReadings.clear(); pendingListModel.clear();
            sendAllBtn.setEnabled(false);

            new Thread(() -> {
                StreamObserver<SensorSummary> responseObserver = new StreamObserver<SensorSummary>() {
                    @Override
                    public void onNext(SensorSummary summary) {
                        SwingUtilities.invokeLater(() -> {
                            sensorOutputArea.append("=== streamSensorReadings() CLIENT STREAMING RESPONSE ===\n");
                            sensorOutputArea.append("Status:            " + summary.getStatus()             + "\n");
                            sensorOutputArea.append("Readings Received: " + summary.getReadingsReceived()   + "\n");
                            sensorOutputArea.append("Readings Saved:    " + summary.getReadingsSaved()      + "\n");
                            sensorOutputArea.append("Avg Temperature:   " + summary.getAvgTemperature()     + "C\n");
                            sensorOutputArea.append("Max Temperature:   " + summary.getMaxTemperature()     + "C\n");
                            sensorOutputArea.append("Avg Humidity:      " + summary.getAvgHumidity()        + "%\n");
                            sensorOutputArea.append("Min Humidity:      " + summary.getMinHumidity()        + "%\n");
                            sensorOutputArea.append("Avg Wind Speed:    " + summary.getAvgWindSpeed()       + " km/h\n");
                            sensorOutputArea.append("Risk Assessment:   " + summary.getRiskAssessment()     + "\n");
                            sensorOutputArea.append("Risk Reasoning:    " + summary.getRiskReasoning()      + "\n");
                            sensorOutputArea.append("Flagged Sensors:   " + summary.getFlaggedSensorsList() + "\n");
                            sensorOutputArea.append("=======================================================\n");
                        });
                    }
                    @Override
                    public void onError(Throwable t) {
                        // ERROR HANDLING: display gRPC status code from the server response
                        SwingUtilities.invokeLater(() -> {
                            if (t instanceof StatusRuntimeException) {
                                StatusRuntimeException sre = (StatusRuntimeException) t;
                                sensorOutputArea.append("gRPC ERROR [" + sre.getStatus().getCode()
                                        + "]: " + sre.getStatus().getDescription() + "\n");
                            } else {
                                sensorOutputArea.append("ERROR: " + t.getMessage() + "\n");
                            }
                        });
                    }
                    @Override
                    public void onCompleted() {
                        SwingUtilities.invokeLater(() ->
                                sensorOutputArea.append("=== Stream complete ===\n"));
                    }
                };

                StreamObserver<SensorReading> requestObserver =
                        sensorAsyncStub.streamSensorReadings(responseObserver);

                try {
                    SwingUtilities.invokeLater(() ->
                            sensorOutputArea.append("--- Streaming " + toSend.size()
                                    + " reading(s) to server ---\n"));

                    for (SensorReading r : toSend) {
                        requestObserver.onNext(r);
                        SwingUtilities.invokeLater(() ->
                                sensorOutputArea.append("Sent: " + r.getSensorId()
                                        + " | Temp: "     + r.getTemperature() + "C"
                                        + " | Humidity: " + r.getHumidity()    + "%"
                                        + " | CO2: "      + r.getCo2Ppm()      + "ppm\n"));
                        Thread.sleep(300);
                    }
                    requestObserver.onCompleted();

                } catch (Exception ex) {
                    requestObserver.onError(ex);
                    SwingUtilities.invokeLater(() ->
                            sensorOutputArea.append("ERROR: " + ex.getMessage() + "\n"));
                }
            }).start();
        });

        clearOutputBtn.addActionListener(e -> sensorOutputArea.setText(""));
        return panel;
    }

    // ClimateCoordinationService
    private JPanel buildCoordinationTab() {
        JPanel panel = new JPanel(new BorderLayout(5, 5));

        JPanel inputPanel = new JPanel(new GridLayout(5, 2, 5, 5));
        inputPanel.setBorder(BorderFactory.createTitledBorder("Coordination Message Inputs"));

        JTextField agencyField = new JTextField("Lagos Fire Service");
        JTextField regionField = new JTextField("ZONE-A-LAGOS");
        JTextField eventField  = new JTextField("WILDFIRE");
        JTextField updateField = new JTextField("Fire front advancing north at 60km/h");
        JSpinner prioritySpin  = new JSpinner(new SpinnerNumberModel(3, 1, 4, 1));

        inputPanel.add(new JLabel("Agency Name:"));    inputPanel.add(agencyField);
        inputPanel.add(new JLabel("Region ID:"));      inputPanel.add(regionField);
        inputPanel.add(new JLabel("Event Type:"));     inputPanel.add(eventField);
        inputPanel.add(new JLabel("Status Update:"));  inputPanel.add(updateField);
        inputPanel.add(new JLabel("Priority (1-4):")); inputPanel.add(prioritySpin);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton openStreamBtn  = new JButton("Open Coordination Stream (Bidi)");
        JButton sendMsgBtn     = new JButton("Send Update");
        JButton closeBtn       = new JButton("Close Stream");
        JButton clearOutputBtn = new JButton("Clear Output");
        sendMsgBtn.setEnabled(false);
        closeBtn.setEnabled(false);
        buttonPanel.add(openStreamBtn);
        buttonPanel.add(sendMsgBtn);
        buttonPanel.add(closeBtn);
        buttonPanel.add(clearOutputBtn);

        coordinationOutputArea = new JTextArea();
        coordinationOutputArea.setEditable(false);
        coordinationOutputArea.setFont(new Font("Monospaced", Font.PLAIN, 12));

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(inputPanel,  BorderLayout.CENTER);
        topPanel.add(buttonPanel, BorderLayout.SOUTH);
        panel.add(topPanel,                                BorderLayout.NORTH);
        panel.add(new JScrollPane(coordinationOutputArea), BorderLayout.CENTER);

        openStreamBtn.addActionListener((ActionEvent e) -> {
            if (coordinationAsyncStub == null) {
                coordinationOutputArea.append("ERROR: CoordinationService not connected yet.\n"); return;
            }

            StreamObserver<CoordinationResponse> responseObserver = new StreamObserver<CoordinationResponse>() {
                @Override
                public void onNext(CoordinationResponse response) {
                    SwingUtilities.invokeLater(() -> {
                        coordinationOutputArea.append("=== SERVER RESPONSE ===\n");
                        coordinationOutputArea.append("Region:      " + response.getRegionId()           + "\n");
                        coordinationOutputArea.append("Risk Level:  " + response.getWildfireRiskLevel()  + "\n");
                        coordinationOutputArea.append("Action:      " + response.getRecommendedAction()  + "\n");
                        coordinationOutputArea.append("Resources:   " + response.getResourceAllocation() + "\n");
                        coordinationOutputArea.append("Spread:      " + response.getPredictedSpread()    + "\n");
                        coordinationOutputArea.append("Evacuation:  " + response.getEvacuationRequired()
                                + (response.getEvacuationRequired()
                                   ? " | Radius: " + response.getEvacuationRadiusKm() + "km" : "") + "\n");
                        coordinationOutputArea.append("Notes:       " + response.getCoordinationNotes()  + "\n");
                        coordinationOutputArea.append("=======================\n");
                    });
                }
                @Override
                public void onError(Throwable t) {
                    // ERROR HANDLING: display gRPC status code from the server
                    SwingUtilities.invokeLater(() -> {
                        if (t instanceof StatusRuntimeException) {
                            StatusRuntimeException sre = (StatusRuntimeException) t;
                            coordinationOutputArea.append("gRPC ERROR [" + sre.getStatus().getCode()
                                    + "]: " + sre.getStatus().getDescription() + "\n");
                        } else {
                            coordinationOutputArea.append("ERROR: " + t.getMessage() + "\n");
                        }
                        sendMsgBtn.setEnabled(false);
                        closeBtn.setEnabled(false);
                        openStreamBtn.setEnabled(true);
                    });
                }
                @Override
                public void onCompleted() {
                    SwingUtilities.invokeLater(() -> {
                        coordinationOutputArea.append("=== Coordination stream closed ===\n");
                        sendMsgBtn.setEnabled(false);
                        closeBtn.setEnabled(false);
                        openStreamBtn.setEnabled(true);
                    });
                }
            };

            coordinationRequestObserver =
                    coordinationAsyncStub.coordinateResponse(responseObserver);

            coordinationOutputArea.append("=== Coordination stream opened ===\n");
            openStreamBtn.setEnabled(false);
            sendMsgBtn.setEnabled(true);
            closeBtn.setEnabled(true);
        });

        sendMsgBtn.addActionListener((ActionEvent e) -> {
            if (coordinationRequestObserver == null) {
                coordinationOutputArea.append(
                        "ERROR: Stream not open. Click Open Coordination Stream first.\n"); return;
            }
            if (agencyField.getText().trim().isEmpty() || regionField.getText().trim().isEmpty()) {
                coordinationOutputArea.append(
                        "ERROR: Agency Name and Region ID are required fields.\n"); return;
            }

            CoordinationMessage message = CoordinationMessage.newBuilder()
                    .setMessageId("MSG-" + System.currentTimeMillis())
                    .setSenderOrg(agencyField.getText().trim())
                    .setSenderId("UNIT-01")
                    .setPriority((int) prioritySpin.getValue())
                    .setRegionId(regionField.getText().trim())
                    .setEventType(eventField.getText().trim())
                    .setStatusUpdate(updateField.getText().trim())
                    .setFireSpreadKm(4.7f)
                    .setResourcesNeeded(true)
                    .setResourceType("AERIAL_SUPPORT")
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            coordinationRequestObserver.onNext(message);
            coordinationOutputArea.append("Sent: [" + agencyField.getText().trim()
                    + "] Priority " + prioritySpin.getValue()
                    + " | " + updateField.getText().trim() + "\n");
        });

        closeBtn.addActionListener((ActionEvent e) -> {
            if (coordinationRequestObserver != null) {
                coordinationRequestObserver.onCompleted();
                coordinationRequestObserver = null;
                coordinationOutputArea.append("=== Stream closed by client ===\n");
                sendMsgBtn.setEnabled(false);
                closeBtn.setEnabled(false);
                openStreamBtn.setEnabled(true);
            }
        });

        clearOutputBtn.addActionListener(e -> coordinationOutputArea.setText(""));
        return panel;
    }

    // main()
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            WildfireMonitorGUI gui = new WildfireMonitorGUI();
            gui.setVisible(true);
        });
    }
}