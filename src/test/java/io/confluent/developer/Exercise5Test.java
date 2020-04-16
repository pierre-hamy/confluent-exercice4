package io.confluent.developer;

import com.google.gson.JsonObject;
import io.confluent.developer.interactivequery.MessageService;
import io.confluent.developer.serde.JsonObjectSerde;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class Exercise5Test {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> messageSentTopic;
    private TestInputTopic<String, String> messageReceivedTopic;
    private TestOutputTopic<String, String> alertDelayedMessageTopic;
    private MessageService messageService;
    private Client client;

    @BeforeEach
    void init() throws Exception {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_unit_test");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Main.sendAlertIfMissingMessage(streamsBuilder);

        driver = new TopologyTestDriver(streamsBuilder.build(), properties);
        messageSentTopic = driver.createInputTopic("message-sent", new StringSerializer(), new StringSerializer());
        messageReceivedTopic = driver.createInputTopic("message-received", new StringSerializer(), new StringSerializer());
        alertDelayedMessageTopic = driver.createOutputTopic("missing-response-alert", new StringDeserializer(), new StringDeserializer());
        messageService = new MessageService(driver);
        messageService.start();
        client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    }

    @AfterEach
    void destroy() {
        try{
            messageService.stop();
            driver.close();
        }catch(Exception e){
            try {
                FileUtils.deleteDirectory(new File("C:/tmp/kafka-streams")); //there is a bug on Windows that does not delete the state directory properly. In order for the test to pass, the directory must be deleted manually
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    // Test where we send a message to the external application, and the application
    // is replying to us 10 seconds later
    // It should not trigger any alert
    @Test
    void everythingOk() {
        messageSentTopic.pipeInput("m1", "whatever");
        driver.advanceWallClockTime(Duration.ofSeconds(10));
        messageReceivedTopic.pipeInput("m1", "ok to your whatever!");
        assertTrue(alertDelayedMessageTopic.isEmpty());
        String result = client
                .target("http://localhost:8080/get/m1")
                .request(MediaType.TEXT_PLAIN)
                .get(new GenericType<String>() {
                });
        assertEquals("no entry", result);
    }

    // Test where we send two message.
    // We have a response for the first message after 10 seconds (ok),
    // but we do not have a response for the second message
    // It should trigger an alert
    @Test
    void everythingNotOk() {
        messageSentTopic.pipeInput("m1", "whatever");
        messageSentTopic.pipeInput("m2", "whatever 2");
        driver.advanceWallClockTime(Duration.ofSeconds(10));
        messageReceivedTopic.pipeInput("m1", "ok to your whatever!");
        driver.advanceWallClockTime(Duration.ofSeconds(30));
        assertFalse(alertDelayedMessageTopic.isEmpty());
        assertEquals("m2", alertDelayedMessageTopic.readRecord().getKey());
        assertTrue(alertDelayedMessageTopic.isEmpty());

        String result = client
                .target("http://localhost:8080/get/m2")
                .request(MediaType.TEXT_PLAIN)
                .get(new GenericType<String>() {
                });
        assertEquals("whatever 2", result);
    }

    // Test where we send two message.
    // We have a response for the first message after 10 seconds (ok),
    // but we do not have a response for the second message
    // It should trigger an alert
    @Test
    void everythingNotOk2() {
        messageSentTopic.pipeInput("m2", "whatever 2");
        messageSentTopic.pipeInput("m1", "whatever");
        driver.advanceWallClockTime(Duration.ofSeconds(10));
        messageReceivedTopic.pipeInput("m1", "ok to your whatever!");
        driver.advanceWallClockTime(Duration.ofSeconds(30));
        assertFalse(alertDelayedMessageTopic.isEmpty());
        assertEquals("m2", alertDelayedMessageTopic.readRecord().getKey());
        assertTrue(alertDelayedMessageTopic.isEmpty());

        String result = client
                .target("http://localhost:8080/get/m2")
                .request(MediaType.TEXT_PLAIN)
                .get(new GenericType<String>() {
                });
        assertEquals("whatever 2", result);
    }
}