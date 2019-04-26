package com.solace.hybrid_edge_starter.processor;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Component;

import java.sql.*;

@Component
public class MyMqttCallback implements MqttCallback {
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String time = new Timestamp(System.currentTimeMillis()).toString();
        System.out.println("tony mqtt arrived.............................................................");
        System.out.println("\nReceived a Message!" +
            "\n\tTime:    " + time +
            "\n\tTopic:   " + topic +
            "\n\tMessage: " + new String(message.getPayload()) +
            "\n\tQoS:     " + message.getQos() + "\n");
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("Connection to Solace messaging lost!" + cause.getMessage());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
