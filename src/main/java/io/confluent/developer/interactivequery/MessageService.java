package io.confluent.developer.interactivequery;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

@Path("")
public class MessageService {
    private final ReadOnlyKeyValueStore<String, KeyValue<Long, String>> store;
    private final Client client;
    private final MetadataService metadataService;
    private Server jettyServer;
    private HostStoreInfo hostInfo;

    public MessageService(KafkaStreams kafkaStreams) throws InterruptedException {
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            Thread.sleep(100);
        }
        hostInfo = new HostStoreInfo();
        hostInfo.setHost("localhost");
        hostInfo.setPort(8080);
        metadataService = new MetadataService(kafkaStreams);
        store = kafkaStreams.store("inflightMessageStore", QueryableStoreTypes.keyValueStore());
        client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    }

    public MessageService(TopologyTestDriver kafkaStreams) throws InterruptedException {
        hostInfo = new HostStoreInfo();
        hostInfo.setHost("localhost");
        hostInfo.setPort(8080);
        metadataService = null;
        store = kafkaStreams.getKeyValueStore("inflightMessageStore");
        client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    }

    /**
     * If the states are on a different instance of the application
     * forward it to the proper instance. Otherwise, use local queryable
     * state store to fetch the information and reply
     * @return a JSON string representing an array of all message with missing
     * response
     */
    @GET
    @Path("get/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getLateMessage(@PathParam("id") String id) {
        if (metadataService != null) {
            HostStoreInfo hostStoreInfo = metadataService.streamsMetadataForStoreAndKey("inflightMessageStore", id, new StringSerializer());
            if (!thisHost(hostStoreInfo)) {
                return fetchLateMessage(hostStoreInfo, id);
            }
        }

        if (store.get(id) == null) {
           return "no entry";
        }

        // TODO
        // String order = store.get(id).value;
        // return order;
        // ??

        return "remove me once you're done!";
    }


    /**
     * Forward a fetch request to a distant node
     * @param id ID of the message to look for
     * @param host host to forward the request to
     * @return the response of the distant host
     */
    private String fetchLateMessage(HostStoreInfo host, String id) {
        return client.target(String.format("http://%s:%d/%s/%s", host.getHost(), host.getPort(), "get", id))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<String>() {
                });
    }

    /**
     * Start the jetty server on the instance
     * @throws Exception
     */
    public void start() throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server();
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost("localhost");
        connector.setPort(8080);
        jettyServer.addConnector(connector);

        context.start();

        try {
            jettyServer.start();
        } catch (final java.net.SocketException exception) {
            throw new Exception(exception.toString());
        }
    }

    private boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.getHost()) &&
                host.getPort() == hostInfo.getPort();
    }


    /**
     * Stop the Jetty Server
     * @throws Exception from jetty
     */
    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }
}
