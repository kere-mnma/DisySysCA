/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package solutionca.distsysca;

/**
 * SDG 15 - Life on Land
 * Smart Climate and Wildfire Risk Monitoring System
 *
 * Adapted from ExampleServiceRegistration.java provided in class.
 */
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

public class WildfireServiceRegistration {

    private static JmDNS jmdns;
    private static WildfireServiceRegistration theRegister;

    /**
     * WildfireServiceRegistration uses the Singleton pattern. Only one instance
     * of it can exist. The constructor is private. New instances are created by
     * calling the getInstance method. Services can register themselves by
     * invoking registerService. The constructor creates the DNS register object.
     */
    private WildfireServiceRegistration() throws UnknownHostException, IOException {
        jmdns = JmDNS.create(InetAddress.getLocalHost());
        System.out.println("Register: InetAddress.getLocalHost():" + InetAddress.getLocalHost());
    }

    /**
     * Services call getInstance() to get the singleton instance of the register.
     *
     * @return
     * @throws IOException
     */
    public static WildfireServiceRegistration getInstance() throws IOException {
        if (theRegister == null) {
            theRegister = new WildfireServiceRegistration();
        }
        return theRegister;
    }

    /**
     * Services call registerService to register themselves so that clients can
     * discover the service.
     *
     * Each of the three gRPC services calls this method after starting up:
     *   AlertService             - port 50051
     *   ClimateSensorService     - port 50052
     *   ClimateCoordinationService - port 50053
     *
     * @param type - fully qualified service type name, such as _grpc._tcp.local.
     * @param name - unqualified service instance name, such as AlertService
     * @param port - the local port on which the service runs
     * @param text - string describing the service
     * @throws IOException
     */
    public void registerService(String type, String name, int port, String text) throws IOException {

        // Construct a service description for registering with JmDNS
        // Parameters:
        //   type - fully qualified service type name, such as _grpc._tcp.local.
        //   name - unqualified service instance name, such as AlertService
        //   port - the local port on which the service runs
        //   text - string describing the service
        // Returns:
        //   new service info
        ServiceInfo serviceInfo = ServiceInfo.create(type, name, port, text);
        // register the service
        jmdns.registerService(serviceInfo);
        System.out.println("Registered Service " + serviceInfo.toString());
    }
}