package org.otmb.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.support.json.EmbeddedJsonHeadersMessageMapper;
import org.springframework.integration.zeromq.ZeroMqProxy;
import org.springframework.integration.zeromq.channel.ZeroMqChannel;
import org.springframework.integration.zeromq.inbound.ZeroMqMessageProducer;
import org.springframework.integration.zeromq.outbound.ZeroMqMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.zeromq.SocketType;
import org.zeromq.ZContext;

import java.time.Duration;

@Configuration
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
    public ZeroMqProxy zeroMqProxy(
            final ZContext context,
            @Value("${zmq.channel.port.frontend}") int frontendPort,
            @Value("${zmq.channel.port.backend}") int backendPort
    ) {
        ZeroMqProxy proxy = new ZeroMqProxy(context, ZeroMqProxy.Type.SUB_PUB);
        proxy.setExposeCaptureSocket(true);
        proxy.setFrontendPort(frontendPort);
        proxy.setBackendPort(backendPort);

        return proxy;
    }

    @Bean(name = "zeroMqPubChannel")
    ZeroMqChannel zeroMqPubChannel(
            final ZContext context,
            final OtmbItDataMapper otmbItDataMapper,
            final ZeroMqProxy proxy
    ) {
        ZeroMqChannel channel = new ZeroMqChannel(context, true);
        channel.setZeroMqProxy(proxy);
        channel.setConsumeDelay(Duration.ofMillis(100));
        channel.setMessageConverter(new GenericMessageConverter());
        EmbeddedJsonHeadersMessageMapper mapper = new EmbeddedJsonHeadersMessageMapper(otmbItDataMapper.getObjectMapper());
        channel.setMessageMapper(mapper);
        return channel;
    }

    @Bean
    @ServiceActivator(inputChannel = "zeroMqPubChannel")
    public MessageHandler subscribe() {
        return message -> logger.info("Received message: {}", message);
    }

    /*
    @Bean
    @ServiceActivator(inputChannel = "zeroMqPubChannel")
    public ZeroMqMessageHandler zeroMqMessageHandler(ZContext context) {
        ZeroMqMessageHandler messageHandler =
                new ZeroMqMessageHandler(context, "inproc://vip", SocketType.PUB);
        messageHandler.setTopicExpression(
                new FunctionExpression<Message<?>>((message) -> message.getHeaders().get("dnp3/point")));
        messageHandler.setMessageMapper(new EmbeddedJsonHeadersMessageMapper());
        
        return messageHandler;
    }
     */

    @Bean
    public ZeroMqMessageProducer zeroMqMessageProducer(ZContext context, @Qualifier("zeroMqPubChannel") MessageChannel outputChannel) {
        ZeroMqMessageProducer messageProducer = new ZeroMqMessageProducer(context, SocketType.SUB);
        messageProducer.setConnectUrl("inproc://vip");
        messageProducer.setOutputChannel(outputChannel);
        messageProducer.setTopics("dnp3/point");
        messageProducer.setReceiveRaw(true);
        //messageProducer.setBindPort(6060);
        messageProducer.setConsumeDelay(Duration.ofMillis(100));
        return messageProducer;
    }
}
