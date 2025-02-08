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
package org.apache.pulsar.io.kafka;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

/**
 *  Kafka Source that transfers the data from Kafka to Pulsar and sets the Schema type properly.
 *  We use the key and the value deserializer in order to decide the type of Schema to be set on the topic on Pulsar.
 *  In case of KafkaAvroDeserializer and KafkaProtobufDeserializer we use the Schema Registry to download the schema and apply it to the topic.
 *  Please refer to {@link #getSchemaFromDeserializerAndAdaptConfiguration(String, Properties, boolean)} for the list
 *  of supported Deserializers.
 *  If you set StringDeserializer for the key then we use the raw key as key for the Pulsar message.
 *  If you set another Deserializer for the key we use the KeyValue schema type in Pulsar with the SEPARATED encoding.
 *  This way the Key is stored in the Pulsar key, encoded as base64 string and with a Schema, the Value of the message
 *  is stored in the Pulsar value with a Schema.
 *  This way there is a one-to-one mapping between Kafka key/value pair and the Pulsar data model.
 */
@Connector(
    name = "kafka",
    type = IOType.SOURCE,
    help = "Transfer data from Kafka to Pulsar.",
    configClass = KafkaSourceConfig.class
)
@Slf4j
public class KafkaBytesSource extends KafkaAbstractSource<ByteBuffer> {

    private KafkaSchemaCache schemaCache;
    private Schema<ByteBuffer> keySchema;
    private Schema<ByteBuffer> valueSchema;
    private boolean produceKeyValue;
    private static final String SCHEMA_TYPE_CONFIG = "schema.type";

    @Override
    protected Properties beforeCreateConsumer(Properties props) {
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        log.info("Created kafka consumer config : {}", props);

        keySchema = getSchemaFromDeserializerAndAdaptConfiguration(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                props, true);
        valueSchema = getSchemaFromDeserializerAndAdaptConfiguration(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                props, false);

        boolean needsSchemaCache = keySchema instanceof DeferredSchemaPlaceholder
                || valueSchema instanceof DeferredSchemaPlaceholder;

        if (needsSchemaCache) {
            initSchemaCache(props);
        }

        if (keySchema.getSchemaInfo().getType() != SchemaType.STRING) {
            // If the Key is a String we can use native Pulsar Key.
            // Otherwise, we use KeyValue schema.
            // That allows you to set a schema for the Key and a schema for the Value.
            // Using SEPARATED encoding the key is saved into the binary key,
            // so it is used for routing and for compaction.
            produceKeyValue = true;
        }

        return props;
    }

    private void initSchemaCache(Properties props) {
        String urls = props.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        int maxSchemaObject = MAX_SCHEMAS_PER_SUBJECT_DEFAULT;
        String maxSchema = props.getProperty(MAX_SCHEMAS_PER_SUBJECT_CONFIG);
        if (maxSchema != null) {
            try {
                maxSchemaObject = Integer.parseInt(maxSchema);
            } catch (NumberFormatException e) {
                log.warn("Invalid value for maxSchemasPerSubject, using default value: {}", maxSchemaObject);
            }
        }

        List<SchemaProvider> providers = List.of(
                new AvroSchemaProvider(),
                new ProtobufSchemaProvider()
        );

        // we need to provide schema registry required properties, like auth related
        Map<String, Object> configs = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            configs.put(key, props.get(key));
        }

        SchemaRegistryClient schemaRegistryClient =
                new CachedSchemaRegistryClient(urls, maxSchemaObject, providers, configs);
        log.info("initializing SchemaRegistry Client, urls:{}, maxSchemasPerSubject: {}", urls, maxSchemaObject);
        schemaCache = new KafkaSchemaCache(schemaRegistryClient);
    }

    @Override
    public KafkaRecord<ByteBuffer> buildRecord(ConsumerRecord<Object, Object> consumerRecord) {
        if (produceKeyValue) {
            ByteBuffer key = extractSimpleValue(consumerRecord.key());
            ByteBuffer value = extractSimpleValue(consumerRecord.value());
            Schema<ByteBuffer> currentKeySchema = getSchemaFromObject(consumerRecord.key(), keySchema);
            Schema<ByteBuffer> currentValueSchema = getSchemaFromObject(consumerRecord.value(), valueSchema);
            return new KeyValueKafkaRecord<ByteBuffer, ByteBuffer>(consumerRecord,
                    new KeyValue<>(key, value),
                    currentKeySchema,
                    currentValueSchema,
                    copyKafkaHeaders(consumerRecord));

        } else {
            Object value = consumerRecord.value();
            return new KafkaRecord<>(consumerRecord,
                    extractSimpleValue(value),
                    getSchemaFromObject(value, valueSchema),
                    copyKafkaHeaders(consumerRecord));

        }
    }

    private static ByteBuffer extractSimpleValue(Object value) {
        // we have substituted the original Deserializer with
        // ByteBufferDeserializer in order to save memory copies
        // so here we can have only a ByteBuffer or at most a
        // BytesWithKafkaSchema in case of ExtractKafkaSchemaDeserializer
        if (value == null) {
            return null;
        } else if (value instanceof BytesWithKafkaSchema) {
            return ((BytesWithKafkaSchema) value).getValue();
        } else if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        } else {
            throw new IllegalArgumentException("Unexpected type from Kafka: " + value.getClass());
        }
    }

    private Schema<ByteBuffer> getSchemaFromObject(Object value, Schema<ByteBuffer> fallback) {
        if (value instanceof BytesWithKafkaSchema) {
            // this is a Struct with schema downloaded by the schema registry
            // the schema may be different from record to record
            return schemaCache.get(((BytesWithKafkaSchema) value).getSchemaId(),
                    ((BytesWithKafkaSchema) value).getSchemaType());
        } else {
            return fallback;
        }
    }

    private static Schema<ByteBuffer> getSchemaFromDeserializerAndAdaptConfiguration(String key, Properties props,
                                                                                     boolean isKey) {
        String kafkaDeserializerClass = props.getProperty(key);
        Objects.requireNonNull(kafkaDeserializerClass);

        // we want to simply transfer the bytes,
        // by default we override the Kafka Consumer configuration
        // to pass the original ByteBuffer
        props.put(key, ByteBufferDeserializer.class.getCanonicalName());

        Schema<?> result;
        if (ByteArrayDeserializer.class.getName().equals(kafkaDeserializerClass)
            || ByteBufferDeserializer.class.getName().equals(kafkaDeserializerClass)
            || BytesDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.BYTEBUFFER;
        } else if (StringDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            if (isKey) {
                // for the key we use the String value, and we want StringDeserializer
                props.put(key, kafkaDeserializerClass);
            }
            result = Schema.STRING;
        } else if (DoubleDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.DOUBLE;
        } else if (FloatDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.FLOAT;
        } else if (IntegerDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.INT32;
        } else if (LongDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.INT64;
        } else if (ShortDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.INT16;
        } else if (KafkaAvroDeserializer.class.getName().equals(kafkaDeserializerClass)){
            // in this case we have to inject our custom deserializer
            // that extracts Avro schema information
            props.put(key, ExtractKafkaSchemaDeserializer.class.getName());
            props.put(SCHEMA_TYPE_CONFIG, SchemaType.AVRO.name());
            // this is only a placeholder, we are not really using AUTO_PRODUCE_BYTES
            // but we the schema is created by downloading the definition from the SchemaRegistry
            return DeferredSchemaPlaceholder.INSTANCE;
        } else if (KafkaProtobufDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            // in this case we have to inject our custom deserializer
            // that extracts Protobuf schema information
            props.put(key, ExtractKafkaSchemaDeserializer.class.getName());
            props.put(SCHEMA_TYPE_CONFIG, SchemaType.PROTOBUF_NATIVE.name());
            // this is only a placeholder, we are not really using AUTO_PRODUCE_BYTES
            // but we the schema is created by downloading the definition from the SchemaRegistry
            return DeferredSchemaPlaceholder.PROTOBUF_INSTANCE;
        } else {
            throw new IllegalArgumentException("Unsupported deserializer " + kafkaDeserializerClass);
        }
        return new ByteBufferSchemaWrapper(result);
    }

    Schema<ByteBuffer> getKeySchema() {
        return keySchema;
    }

    Schema<ByteBuffer> getValueSchema() {
        return valueSchema;
    }

    boolean isProduceKeyValue() {
        return produceKeyValue;
    }


    public static class ExtractKafkaSchemaDeserializer implements Deserializer<BytesWithKafkaSchema> {
        SchemaType schemaType = SchemaType.AVRO;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            if (configs.containsKey(SCHEMA_TYPE_CONFIG)) {
                schemaType = SchemaType.valueOf((String) configs.get(SCHEMA_TYPE_CONFIG));
            }
        }

        @Override
        public BytesWithKafkaSchema deserialize(String topic, byte[] payload) {
            if (payload == null) {
                return null;
            } else {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(payload);
                    buffer.get(); // magic number
                    int id = buffer.getInt();
                    // the kafka protobuf serializer encodes the MessageIndexes in the payload, we need to skip them
                    // the indexes are encoded as varint like below:
                    // ByteUtils.writeVarint(indexes.size(), buffer);
                    //
                    // for(Integer index : indexes) {
                    //     ByteUtils.writeVarint(index, buffer);
                    // }
                    if (schemaType == SchemaType.PROTOBUF_NATIVE) {
                        int size = ByteUtils.readVarint(buffer);
                        for (int i = 0; i < size; i++) {
                            ByteUtils.readVarint(buffer);
                        }
                    }
                    return new BytesWithKafkaSchema(buffer, id, schemaType);
                } catch (Exception err) {
                    throw new SerializationException("Error deserializing Kafka message", err);
                }
            }
        }
    }

    static final class DeferredSchemaPlaceholder extends ByteBufferSchemaWrapper {
        DeferredSchemaPlaceholder(SchemaType schemaType) {
            super(SchemaInfoImpl
                    .builder()
                    .type(schemaType)
                    .properties(Collections.emptyMap())
                    .schema(new byte[0])
                    .build());
        }

        static final DeferredSchemaPlaceholder INSTANCE = new DeferredSchemaPlaceholder(SchemaType.AVRO);
        static final DeferredSchemaPlaceholder PROTOBUF_INSTANCE = new DeferredSchemaPlaceholder(SchemaType.PROTOBUF_NATIVE);
    }

}