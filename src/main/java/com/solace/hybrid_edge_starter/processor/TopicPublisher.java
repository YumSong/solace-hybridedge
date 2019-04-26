package com.solace.hybrid_edge_starter.processor;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class TopicPublisher {
    public void run(String content) throws MqttException {
        System.out.println("TopicPublisher initializing...");

        String host = "tcp://localhost:8888";
        String username = "default";
        String password = "default";

        try {
            // Create an Mqtt client
            MqttClient mqttClient = new MqttClient(host, "HelloWorldPub");
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());

            // Connect the client
            System.out.println("Connecting to Solace messaging at " + host);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            // Create a Mqtt message
            MqttMessage message = new MqttMessage(content.getBytes());
            // Set the QoS on the Messages -
            // Here we are using QoS of 0 (equivalent to Direct Messaging in Solace)
            message.setQos(0);

            System.out.println("Publishing message: " + content);

            // Publish the message
            mqttClient.publish("T/GettingStarted/pubsub", message);

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }
}
