/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.hybrid_edge_starter.processor;
import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * A Mqtt topic subscriber
 *
 */
@Configuration
public class TopicSubscriberConfig {


    @Bean
    public MqttClient getClient(MqttCallback callback) throws MqttException {
        String host = "tcp://localhost:8888";
        String username = "default";
        String password = "default";
        MqttClient mqttClient = new MqttClient(host, "HelloWorldSub");
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(username);
        connOpts.setPassword(password.toCharArray());
        System.out.println("Connecting to Solace messaging at "+host);
        mqttClient.connect(connOpts);
        System.out.println("Connected");
        mqttClient.setCallback(callback);
        // Topic filter the client will subscribe to
        final String subTopic = "T/GettingStarted/pubsub";
        System.out.println("Subscribing client to topic: " + subTopic);
        mqttClient.subscribe(subTopic, 0);
        return mqttClient;
    }

    public static void main(String[] args) {
        new TopicSubscriberConfig();
    }
}
