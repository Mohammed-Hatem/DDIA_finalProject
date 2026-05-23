package com.ddia.openmeteo.config;

import com.ddia.openmeteo.model.Weather;
import com.ddia.openmeteo.model.WeatherStatusMessage;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.handler.advice.IdempotentReceiverInterceptor;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.selector.MetadataStoreSelector;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.atomic.AtomicLong;

@Configuration
public class IntegrationConfig {

    @Value("${openmeteo.url:https://api.open-meteo.com/v1/forecast?latitude=31.20&longitude=29.91&current_weather=true&hourly=relativehumidity_2m}")
    private String openMeteoUrl;

    @Value("${kafka.topic:weather_status}")
    private String kafkaTopic;

    private final AtomicLong seqNo = new AtomicLong(0);

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    // 1. Polling Consumer: Fetch from Open-Meteo
    @Bean
    public MessageSource<JsonNode> openMeteoSource(RestTemplate restTemplate) {
        return () -> {
            try {
                JsonNode response = restTemplate.getForObject(openMeteoUrl, JsonNode.class);
                return org.springframework.messaging.support.MessageBuilder.withPayload(response).build();
            } catch (Exception e) {
                return null;
            }
        };
    }

    // Channels
    @Bean
    public MessageChannel invalidMessageChannel() {
        return MessageChannels.direct().getObject();
    }
    
    @Bean
    public MessageChannel deadLetterChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    public ConcurrentMetadataStore metadataStore() {
        return new SimpleMetadataStore();
    }

    // 5. Idempotent Receiver - one metadata entry per s_no
    @Bean
    public IdempotentReceiverInterceptor idempotentReceiverInterceptor() {
        MetadataStoreSelector selector = new MetadataStoreSelector(
            message -> String.valueOf(((WeatherStatusMessage) message.getPayload()).getSNo()),
            message -> "seen",
            metadataStore()
        );
        IdempotentReceiverInterceptor interceptor = new IdempotentReceiverInterceptor(selector);
        interceptor.setDiscardChannel(invalidMessageChannel());
        return interceptor;
    }

    // Flow
    @Bean
    public IntegrationFlow weatherFlow(KafkaTemplate<String, WeatherStatusMessage> kafkaTemplate) {
        return IntegrationFlow.from(openMeteoSource(restTemplate()),
                        c -> c.poller(Pollers.fixedDelay(10000))) // Poll every 10 seconds
                // 3. Filter: Ensure current_weather is present
                .filter(JsonNode.class, node -> node != null && node.has("current_weather"), 
                        f -> f.discardChannel("invalidMessageChannel"))
                // 2. Envelope Wrapper: Transform raw JSON to WeatherStatusMessage
                .transform(JsonNode.class, this::transformToDomain)
                // 5. Idempotent Receiver (Interceptor on this endpoint)
                .filter(WeatherStatusMessage.class, p -> true, 
                        e -> e.advice(idempotentReceiverInterceptor()))
                // 6. Channel Adapter: Push to Kafka
                .handle(kafkaMessageHandler(kafkaTemplate), e -> e.advice(errorHandler()))
                .get();
    }

    private WeatherStatusMessage transformToDomain(JsonNode root) {
        JsonNode current = root.get("current_weather");

        WeatherStatusMessage msg = new WeatherStatusMessage();
        msg.setStationId(999L); // Magic ID for Open-Meteo
        msg.setSNo(seqNo.incrementAndGet());
        msg.setBatteryStatus("high"); // Virtual station has high battery
        msg.setStatusTimestamp(System.currentTimeMillis() / 1000);
        msg.setMessageDropped(false);

        Weather weather = new Weather();
        weather.setTemperature(current.get("temperature").asInt());
        weather.setWindSpeed(current.get("windspeed").asInt());
        weather.setHumidity(resolveHumidity(root, current));

        msg.setWeather(weather);
        return msg;
    }

    private int resolveHumidity(JsonNode root, JsonNode current) {
        if (root.has("hourly") && root.get("hourly").has("relativehumidity_2m")) {
            JsonNode humiditySeries = root.get("hourly").get("relativehumidity_2m");
            if (humiditySeries.isArray() && !humiditySeries.isEmpty()) {
                return humiditySeries.get(0).asInt();
            }
        }
        return 50;
    }

    @Bean
    public MessageHandler kafkaMessageHandler(KafkaTemplate<String, WeatherStatusMessage> kafkaTemplate) {
        KafkaProducerMessageHandler<String, WeatherStatusMessage> handler =
                new KafkaProducerMessageHandler<>(kafkaTemplate);
        handler.setTopicExpression(new SpelExpressionParser().parseExpression("'" + kafkaTopic + "'"));
        handler.setMessageKeyExpression(new SpelExpressionParser().parseExpression("payload.stationId.toString()"));
        return handler;
    }

    // Error handling advice for Dead-Letter Channel
    @Bean
    public org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice errorHandler() {
        org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice advice =
            new org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice();
        advice.setFailureChannelName("deadLetterChannel");
        advice.setTrapException(true);
        return advice;
    }

    @ServiceActivator(inputChannel = "invalidMessageChannel")
    public void handleInvalid(org.springframework.messaging.Message<?> message) {
        Object payload = message.getPayload();
        if (payload instanceof WeatherStatusMessage msg) {
            System.out.println("[InvalidMessageChannel] Discarded station="
                    + msg.getStationId() + " s_no=" + msg.getSNo());
        } else {
            System.out.println("[InvalidMessageChannel] Discarded: " + payload);
        }
    }

    @ServiceActivator(inputChannel = "deadLetterChannel")
    public void handleDeadLetter(org.springframework.messaging.Message<?> message) {
        System.out.println("[DeadLetterChannel] Failed to process/send message: " + message.getPayload());
    }
}
