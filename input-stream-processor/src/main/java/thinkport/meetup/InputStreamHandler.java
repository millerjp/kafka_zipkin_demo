package thinkport.meetup;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import thinkport.meetup.avro.Users;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.kafka11.KafkaSender;

import java.util.Objects;
import java.util.Properties;

public class InputStreamHandler {

	private static final Logger LOGGER = LogManager.getLogger(InputStreamHandler.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final Config config = ConfigFactory.load();

	public static void main(String[] args) {

		final String kafkaBootstrapServers = config.getString("kafka.bootstrap-servers");

		LOGGER.info("initialize tracer");

		/* START TRACING INSTRUMENTATION */
		final Tracing tracing = configureTracing(kafkaBootstrapServers);
		final KafkaStreamsTracing kafkaStreamsTracing = KafkaStreamsTracing
				.create(tracing);
		/* END TRACING INSTRUMENTATION */

		LOGGER.info("setup stream");
		final Properties streamsConfig = prepareConfiguration(kafkaBootstrapServers);
		final StreamsBuilder builder = new StreamsBuilder();

		builder.stream(config.getString("topics.input-user-json"),
				Consumed.with(Serdes.String(), Serdes.String()))
				.transform(kafkaStreamsTracing.map("input-parseJson",
						InputStreamHandler::parseJson))
				.filterNot((k, v) -> Objects.isNull(v))
				.transformValues(kafkaStreamsTracing.mapValues("input-transformGender",
						InputStreamHandler::tranformUser))
				.to(config.getString("topics.output-user-avro"));

		final Topology topology = builder.build();
		final KafkaStreams kafkaStreams = kafkaStreamsTracing.kafkaStreams(topology,
				streamsConfig);

		// start the stream
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

	private static Users tranformUser(JsonNode jsonNode) {

		String shortGender = shortenGender(jsonNode.at("/payload/gender").textValue());

		Users user = Users.newBuilder()
				.setRegistertime(jsonNode.at("/payload/registertime").longValue())
				.setRegionid(jsonNode.at("/payload/regionid").textValue())
				.setUserid(jsonNode.at("/payload/userid").textValue())
				.setGender(shortGender).build();
		brave.Span span = Tracing.currentTracer().currentSpan();

		LOGGER.info("transform User:" + user.getUserid().toString());
		span.tag("user.id", user.getUserid().toString());
		span.tag("user.gender", user.getGender().toString());

		return user;
	}

	private static KeyValue<String, JsonNode> parseJson(String key, String value) {
		try {
			return KeyValue.pair(key, OBJECT_MAPPER.readTree(value));
		}
		catch (Exception e) {
			e.printStackTrace();
			return KeyValue.pair(key, null);
		}
	}

	private static String shortenGender(String gender) {
		LOGGER.info("Shorten Gender:" + gender);

		switch (gender) {
		case "MALE":
			return "M";
		case "FEMALE":
			return "F";
		case "OTHER":
			return "O";
		default:
			return "NULL";
		}
	}

	private static Tracing configureTracing(String kafkaBootstrapServers) {

		final KafkaSender sender = KafkaSender.newBuilder()
				.bootstrapServers(kafkaBootstrapServers).build();
		final AsyncReporter<Span> reporter = AsyncReporter.builder(sender).build();

		final Tracing tracing = Tracing.newBuilder()
				.currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
						.addScopeDecorator(ThreadContextScopeDecorator.create()).build())
				.localServiceName(config.getString("zipkin.local-service-name "))
				.sampler(Sampler.ALWAYS_SAMPLE).spanReporter(reporter).build();
		return tracing;
	}

	private static Properties prepareConfiguration(String kafkaBootstrapServers) {
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-input-v1");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				kafkaBootstrapServers);
		streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
				config.getString("schema-registry.url"));
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				SpecificAvroSerde.class);
		streamsConfiguration.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest");
		streamsConfiguration.putIfAbsent(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");

		return streamsConfiguration;
	}

}
