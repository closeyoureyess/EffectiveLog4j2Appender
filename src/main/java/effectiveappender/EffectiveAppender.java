package effectiveappender;

import effectiveappender.config.PropertiesCreator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.Serializable;
import java.util.Properties;

@Plugin(name = "EffectiveAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class EffectiveAppender extends AbstractAppender {

    private final Properties localProperties;

    private final String topic;

    protected EffectiveAppender(String name, String topic, String bootstrapServers,
                                Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions,
                                Property[] properties) {
        super(name, filter, layout, ignoreExceptions, properties);
        localProperties = new PropertiesCreator().createProperties(bootstrapServers);
        this.topic = topic;
    }

    @PluginFactory
    public static EffectiveAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter
    ) {
        return new EffectiveAppender(name, topic, bootstrapServers, filter, layout, false, null);
    }

    @Override
    public void append(LogEvent event) {
        String message = getLayout().toSerializable(event).toString();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(localProperties);) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
            producer.send(producerRecord);
        }
    }
}
