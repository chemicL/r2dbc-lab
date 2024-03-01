package dev.jedrzejczyk.r2dbclab;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.test.MockConnection;
import io.r2dbc.spi.test.MockConnectionFactory;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockStatement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.util.RaceTestUtils;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

public class RacingQueriesTest {

	private static final Logger logger = LoggerFactory.getLogger(RacingQueriesTest.class);

	@Test
	void testRace() throws InterruptedException {
		logger.info("Started");
		CountDownLatch goodLatch = new CountDownLatch(1);
		CountDownLatch badLatch = new CountDownLatch(1);

		ConnectionPool pooledConnectionFactory = connectionPool(slowConnectionFactory());

		RaceTestUtils.race(
				() -> goodActorSimple(pooledConnectionFactory, goodLatch).subscribe(),
				() -> badActorSimple(pooledConnectionFactory, badLatch).subscribe());
//				() -> goodActorSimple(pooledConnectionFactory, badLatch).subscribe());

		goodLatch.await();
		badLatch.await();

//		RaceTestUtils.race(
//				() -> goodActor(pooledConnectionFactory, goodLatch).subscribe(),
//				() -> badActor(pooledConnectionFactory, badLatch).subscribe());
////				() -> goodActor(pooledConnectionFactory, badLatch).subscribe());
//
//		goodLatch.await();
//		badLatch.await();

		System.out.println("Done");
	}

	private Mono<Void> badActorSimple(ConnectionPool pool, CountDownLatch latch) {
		return goodActorSimple(pool, latch).zipWith(
				                             Mono.error(RuntimeException::new).delaySubscription(Duration.ofMillis(50)))
		                             .onErrorComplete()
		                             .map(Tuple2::getT1)
		                             .doOnNext(count -> latch.countDown());
	}

	private Mono<Void> goodActorSimple(ConnectionPool pool, CountDownLatch latch) {
		return pool.create()
		           .flatMap(con -> Mono.from(con.createStatement("SELECT 1")
		                                        .execute())
		                               .flatMap(result -> Mono.from(result.getRowsUpdated()))
		                               .doOnNext(count -> latch.countDown())
		                               .then(Mono.from(con.close()))
		                               .doOnCancel(() -> Mono.from(con.close()).doFinally(s -> latch.countDown()).subscribe())
		    );
	}

	private Mono<Long> goodActor(ConnectionPool pool, CountDownLatch latch) {
		return Mono.usingWhen(pool.create(),
				           con -> Mono.from(con.createStatement("SELECT 1").execute()),
				           Connection::close)
		           .flatMap(result -> Mono.from(result.getRowsUpdated()))
				.doOnNext(count -> latch.countDown());
	}

	private Mono<Long> badActor(ConnectionPool pool, CountDownLatch latch) {
		return goodActor(pool, latch).zipWith(
				Mono.error(RuntimeException::new).delaySubscription(Duration.ofMillis(50)))
		                             .onErrorComplete()
		                             .map(Tuple2::getT1)
		                             .doOnNext(count -> latch.countDown());
	}

	private ConnectionFactory slowConnectionFactory() {
		Random random = new Random();

		Supplier<Result.Segment> supplier = () ->
				MockResult.row(MockRow.builder()
				                      .identified(random.nextInt(), Integer.class,
						                      random.nextInt()).build());
		Stream<Result.Segment> segments =
				Stream.concat(Stream.<Result.Segment>generate(supplier)
				                    .limit(10), Stream.of(MockResult.updateCount(100)));

		List<Result.Segment> segmentsList = segments.toList();

		Flux<Result.Segment> segmentsFlux =
				Flux.fromIterable(segmentsList)
				    .delayElements(Duration.ofMillis(100));

		MockStatement statement =
				MockStatement.builder().result(new MockResult(segmentsFlux)).build();

		Connection connection =
				MockConnection.builder().valid(true).statement(statement).build();

		ConnectionFactory cf =
				MockConnectionFactory.builder().connection(connection).build();

		return cf;
	}

	private ConnectionPool connectionPool(ConnectionFactory cf) {
		ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(cf).minIdle(1).maxSize(1)
		                                                                    .acquireRetry(0).build();

		return new ConnectionPool(poolConfig);
	}

	public final class MockResult implements Result {

		private final Flux<Segment> segments;

		MockResult(Flux<Segment> segments) {
			this.segments = Objects.requireNonNull(segments, "segments must not be null");
		}

		@Override
		public Flux<Long> getRowsUpdated() {
			return this.segments.filter(UpdateCount.class::isInstance)
			                    .cast(UpdateCount.class)
			                    .map(UpdateCount::value)
			                    .collect(Collectors.summingLong(Long::longValue))
			                    .flux();
		}

		@Override
		public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
			Objects.requireNonNull(mappingFunction, "mappingFunction must not be null");

			return this.segments.filter(RowSegment.class::isInstance)
			                    .cast(RowSegment.class)
			                    .map(it -> mappingFunction.apply(it.row(),
					                    it.row()
					                      .getMetadata()));
		}

		@Override
		public <T> Publisher<T> map(Function<? super Readable, ? extends T> mappingFunction) {
			Objects.requireNonNull(mappingFunction, "f must not be null");

			return this.segments.filter(it -> it instanceof RowSegment || it instanceof OutSegment)
			                    .map(it -> {

				                    if (it instanceof OutSegment) {
					                    return mappingFunction.apply(((OutSegment) it).outParameters());
				                    }

				                    return mappingFunction.apply(((RowSegment) it).row());
			                    });
		}

		@Override
		public String toString() {
			return "MockResult{" + "segments=" + this.segments + '}';
		}

		@Override
		public Result filter(Predicate<Segment> filter) {
			Objects.requireNonNull(filter, "mappingFunction must not be null");
			return new MockResult(this.segments.filter(filter));
		}

		@Override
		public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
			Objects.requireNonNull(mappingFunction, "mappingFunction must not be null");

			return this.segments.flatMap(mappingFunction);
		}

		public static UpdateCount updateCount(long value) {
			return () -> value;
		}

		public static RowSegment row(Row row) {
			Objects.requireNonNull(row, "row must not be null");

			return () -> row;
		}

		public static OutSegment outParameters(OutParameters parameters) {
			Objects.requireNonNull(parameters, "parameters must not be null");

			return () -> parameters;
		}
	}
}
