package thinkport.meetup;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import thinkport.meetup.avro.Users;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.kafka11.KafkaSender;

import java.util.Properties;

public class AggStreamHandler {

	private static final Logger LOGGER = LogManager.getLogger(AggStreamHandler.class);

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

		final KStream<String, Users> input = builder
				.stream(config.getString("topics.input-user-avro"));

		input.peek((k, v) -> logTrace(v))
				.to(config.getString("topics.output-user_output"));

		final Topology topology = builder.build();
		final KafkaStreams kafkaStreams = kafkaStreamsTracing.kafkaStreams(topology,
				streamsConfig);

		// start the stream
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

	}

	private static void logTrace(Users user) {

	}

	private static Tracing configureTracing(String kafkaBootstrapServers) {

		final KafkaSender sender = KafkaSender.newBuilder()
				.bootstrapServers(kafkaBootstrapServers).build();
		final AsyncReporter<Span> reporter = AsyncReporter.builder(sender).build();

		final Tracing tracing = Tracing.newBuilder()
				.currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
						.addScopeDecorator(ThreadContextScopeDecorator.create()).build())
				.localServiceName(config.getString("zipkin.local-service-name"))
				.sampler(Sampler.ALWAYS_SAMPLE).spanReporter(reporter).build();
		return tracing;
	}

	private static Properties prepareConfiguration(String kafkaBootstrapServers) {
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
				config.getString("kafka.application-id"));
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
