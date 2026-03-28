/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package solutionca.distsysca;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

public class WildfireServiceRegistration {

    private static JmDNS jmdns;
    private static WildfireServiceRegistration theRegister;

    private WildfireServiceRegistration() throws UnknownHostException, IOException {
        jmdns = JmDNS.create(InetAddress.getLocalHost());
        System.out.println("Register: InetAddress.getLocalHost():" + InetAddress.getLocalHost());
    }

    public static WildfireServiceRegistration getInstance() throws IOException {
        if (theRegister == null) {
            theRegister = new WildfireServiceRegistration();
        }
        return theRegister;
    }

    public void registerService(String type, String name, int port, String text) throws IOException {

        ServiceInfo serviceInfo = ServiceInfo.create(type, name, port, text);
        // register the service
        jmdns.registerService(serviceInfo);
        System.out.println("Registered Service " + serviceInfo.toString());
    }
}