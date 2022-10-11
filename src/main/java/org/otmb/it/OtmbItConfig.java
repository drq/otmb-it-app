package org.otmb.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.support.json.EmbeddedJsonHeadersMessageMapper;
import org.springframework.integration.zeromq.inbound.ZeroMqMessageProducer;
import org.springframework.integration.zeromq.outbound.ZeroMqMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.zeromq.SocketType;
import org.zeromq.ZContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableScheduling
public class OtmbItConfig {
    private static final Logger logger = LoggerFactory.getLogger(OtmbItConfig.class);

    @Bean
    public OtmbItDataMapper otmbItDataMapper() {
        return new OtmbItDataMapper();
    }

    @Bean
    public ZContext zContext() {
        return new ZContext();
    }

    @Bean
    @ServiceActivator(inputChannel = "fluxMessageChannel")
    public MessageHandler subscribe2() {
        return message -> {
            logger.info("=================================================================================");
            logger.info("Received message: {} with headers {}", message.getPayload(), message.getHeaders());
            logger.info("=================================================================================");
        };
    }

    @Bean(name = "fluxMessageChannel")
    public MessageChannel fluxMessageChannel() {
        return new FluxMessageChannel();
    }

    @Bean
    public ZeroMqMessageHandler zeroMqMessageHandler(
            final ZContext context,
            final OtmbItDataMapper otmbItDataMapper,
            @Value("${zmq.channel.pubUrl}") String url
    ) {
        logger.info("Publishing at {}", url);

        ZeroMqMessageHandler messageHandler = new ZeroMqMessageHandler(context, url, SocketType.PUB);

        messageHandler.setSocketConfigurer(socket -> {
            socket.bind(url);
        });

        messageHandler.setTopicExpression(
                new FunctionExpression<Message<?>>((message) -> message.getHeaders().get("topic"))
        );
        messageHandler.setMessageMapper(new EmbeddedJsonHeadersMessageMapper(otmbItDataMapper.getObjectMapper()));

        return messageHandler;
    }

    @Bean
    public ZeroMqMessageProducer zeroMqMessageProducer(
            final ZContext context,
            @Qualifier("fluxMessageChannel") final MessageChannel fluxMessageChannel,
            final OtmbItDataMapper otmbItDataMapper,
            @Value("${zmq.channel.url}") String url,
            @Value("${zmq.channel.topic}") String topic
    ) {
        logger.info("Listening topic {} at {}", topic, url);

        ZeroMqMessageProducer messageProducer = new ZeroMqMessageProducer(context, SocketType.SUB);
        messageProducer.setConnectUrl(url);
        messageProducer.setOutputChannel(fluxMessageChannel);
        messageProducer.setTopics(topic);
        messageProducer.setMessageMapper((object, headers) -> {
            String payload = new String(object);
            int index = payload.indexOf(" ");
            String messageTopic = payload.substring(0, index);
            String json = payload.substring(index + 1);
            Map<String, Object> jsonObj = null;
            try {
                jsonObj = otmbItDataMapper.fromMapJson(json);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            Map<String, Object> messageHeaders = new HashMap<>();
            messageHeaders.put("topic", messageTopic);
            return new GenericMessage<>(jsonObj, messageHeaders);
        });
        messageProducer.setConsumeDelay(Duration.ofMillis(100));

        return messageProducer;
    }

}
