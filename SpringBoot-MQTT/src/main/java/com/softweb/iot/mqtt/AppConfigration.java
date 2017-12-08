package com.softweb.iot.mqtt;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.util.StringUtils;


/**
 * 
 * @author Hardik Shah
 *	This is the main Application Configuration for Spring Integration wih MQTT
 */

@Configuration
public class AppConfigration {

	 	@Value("${mqtt.broker.address}")
	    private String mqttBrokerAddress;

	    @Value("${mqtt.topic}")
	    private String[] mqttTopic;

	    @Value("${mqtt.clientId}")
	    private String mqttClientId;

	    @Value("${mqtt.qos}")
	    private Integer mqttQos;

	    @Value("${mqtt.completionTimeout}")
	    private Integer mqttCompletionTimeout;

	    @Value("${mqtt.username}")
	    private String mqttUsername;

	    @Value("${mqtt.password}")
	    private String mqttPassword;
	    
	    /**
	     *  MQTT Input Channel
	     *  doc regarding Channel 
	     *  http://docs.spring.io/spring-integration/reference/html/messaging-channels-section.html
	     */
	    @Bean
	    public MessageChannel mqttInputChannel() {
	        return new DirectChannel();
	    }

	    /**
	     * Message Producer which connects to MQTT server and consumes the configured mqttTopics
	     * And will place it in mqttMessageEnrichChannel
	     */
	    @Bean
	    public MessageProducer inbound() {
	        MqttPahoMessageDrivenChannelAdapter adapter =
	                new MqttPahoMessageDrivenChannelAdapter(mqttBrokerAddress, mqttClientId,
	                        mqttPahoClientFactory(), mqttTopic);
	        adapter.setCompletionTimeout(mqttCompletionTimeout);
	        adapter.setConverter(new DefaultPahoMessageConverter());
	        adapter.setQos(mqttQos);
	        adapter.setOutputChannel(mqttInputChannel());

	        return adapter;
	    }
	    
	    /**
	     * MQTT Client Factory for connecting with MQTT Server with Authentication
	     */
	    @Bean
	    public MqttPahoClientFactory mqttPahoClientFactory() {
	        DefaultMqttPahoClientFactory mqttPahoClientFactory = new DefaultMqttPahoClientFactory();
	        if (StringUtils.hasText(mqttUsername)) {
	            mqttPahoClientFactory.setUserName(mqttUsername);
	        }
	        if (StringUtils.hasText(mqttPassword)) {
	            mqttPahoClientFactory.setPassword(mqttPassword);
	        }
	        return mqttPahoClientFactory;
	    }
	    
	    /**
	     *  @return read message from MQTT
	     * 	This method read message from MQTT topic
	     * 
	     */
	    
	    @Bean
	    @ServiceActivator(inputChannel = "mqttInputChannel")
	    public MessageHandler handler() {
	    	return new MessageHandler() {

	    		@Override
	    		public void handleMessage(Message<?> message) throws MessagingException {
	    			System.out.println(message.getPayload());
	    		}

	    	};
	    }
	    
}
