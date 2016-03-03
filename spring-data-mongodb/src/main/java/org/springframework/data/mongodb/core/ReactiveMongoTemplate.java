/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.SerializationUtils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.convert.DbRefProxyHandler;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DbRefResolverCallback;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.jca.cci.core.ConnectionCallback;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBRef;
import com.mongodb.MapReduceCommand;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.util.JSONParseException;

import reactor.core.flow.Fuseable.ScalarSupplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.tuple.Tuple2;
import reactor.rx.Stream;

/**
 * Primary implementation of {@link ReactiveMongoOperations}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("deprecation")
public class ReactiveMongoTemplate implements ReactiveMongoOperations, ApplicationContextAware {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMongoTemplate.class);
	private static final String ID_FIELD = "_id";
	private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
	private static final Collection<Class> ITERABLE_CLASSES;

	static {

		Set<Class> iterableClasses = new HashSet<>();
		iterableClasses.add(List.class);
		iterableClasses.add(Collection.class);
		iterableClasses.add(Iterator.class);
		iterableClasses.add(Publisher.class);

		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
	}

	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final ReactiveMongoDbFactory mongoDbFactory;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final QueryMapper queryMapper;
	private final UpdateMapper updateMapper;

	private WriteConcern writeConcern;
	private WriteConcernResolver writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;
	private ReadPreference readPreference;
	private ApplicationEventPublisher eventPublisher;
	private ResourceLoader resourceLoader;
	private MongoPersistentEntityIndexCreator indexCreator;

	/**
	 * Constructor used for a basic template configuration
	 *
	 * @param mongo must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 */
	public ReactiveMongoTemplate(MongoClient mongo, String databaseName) {
		this(new ReactiveMongoDbFactory(mongo, databaseName), null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public ReactiveMongoTemplate(ReactiveMongoDbFactory mongoDbFactory) {
		this(mongoDbFactory, null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mongoConverter
	 */
	public ReactiveMongoTemplate(ReactiveMongoDbFactory mongoDbFactory, MongoConverter mongoConverter) {

		Assert.notNull(mongoDbFactory);

		this.mongoDbFactory = mongoDbFactory;
		this.exceptionTranslator = mongoDbFactory.getExceptionTranslator();
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter(mongoDbFactory) : mongoConverter;
		this.queryMapper = new QueryMapper(this.mongoConverter);
		this.updateMapper = new UpdateMapper(this.mongoConverter);

		// We always have a mapping context in the converter, whether it's a simple one or not
		mappingContext = this.mongoConverter.getMappingContext();
		// We create indexes based on mapping events
		if (null != mappingContext && mappingContext instanceof MongoMappingContext) {

			// TODO
			/*indexCreator = new MongoPersistentEntityIndexCreator((MongoMappingContext) mappingContext, mongoDbFactory);
			eventPublisher = new MongoMappingEventPublisher(indexCreator);
			if (mappingContext instanceof ApplicationEventPublisherAware) {
				((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
			} */
		}
	}

	/**
	 * Configures the {@link WriteResultChecking} to be used with the template. Setting {@literal null} will reset the
	 * default of {@link ReactiveMongoTemplate#DEFAULT_WRITE_RESULT_CHECKING}.
	 *
	 * @param resultChecking
	 */
	public void setWriteResultChecking(WriteResultChecking resultChecking) {
		this.writeResultChecking = resultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : resultChecking;
	}

	/**
	 * Configures the {@link WriteConcern} to be used with the template. If none is configured the {@link WriteConcern}
	 * configured on the {@link MongoDbFactory} will apply. If you configured a {@link Mongo} instance no
	 * {@link WriteConcern} will be used.
	 *
	 * @param writeConcern
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/**
	 * Configures the {@link WriteConcernResolver} to be used with the template.
	 *
	 * @param writeConcernResolver
	 */
	public void setWriteConcernResolver(WriteConcernResolver writeConcernResolver) {
		this.writeConcernResolver = writeConcernResolver;
	}

	/**
	 * Used by @{link {@link #prepareCollection(MongoCollection)} to set the {@link ReadPreference} before any operations
	 * are performed.
	 *
	 * @param readPreference
	 */
	public void setReadPreference(ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		// TODO
		// prepareIndexCreator(applicationContext);

		eventPublisher = applicationContext;
		if (mappingContext instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
		}
		resourceLoader = applicationContext;
	}

	/**
	 * Inspects the given {@link ApplicationContext} for {@link MongoPersistentEntityIndexCreator} and those in turn if
	 * they were registered for the current {@link MappingContext}. If no creator for the current {@link MappingContext}
	 * can be found we manually add the internally created one as {@link ApplicationListener} to make sure indexes get
	 * created appropriately for entity types persisted through this {@link ReactiveMongoTemplate} instance.
	 *
	 * @param context must not be {@literal null}.
	 */
	private void prepareIndexCreator(ApplicationContext context) {

		String[] indexCreators = context.getBeanNamesForType(MongoPersistentEntityIndexCreator.class);

		for (String creator : indexCreators) {
			MongoPersistentEntityIndexCreator creatorBean = context.getBean(creator, MongoPersistentEntityIndexCreator.class);
			if (creatorBean.isIndexCreatorFor(mappingContext)) {
				return;
			}
		}

		if (context instanceof ConfigurableApplicationContext) {
			((ConfigurableApplicationContext) context).addApplicationListener(indexCreator);
		}
	}

	/**
	 * Returns the default {@link MongoConverter}.
	 *
	 * @return
	 */
	public MongoConverter getConverter() {
		return this.mongoConverter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeAsStream(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	// DELETE ME
	public <T> Flux<T> stream(final Query query, final Class<T> entityType) {

		return createFlux(entityType, new ReactiveCollectionCallback<T>() {
			@Override
			public Flux<T> doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

				MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityType);

				Document mappedFields = queryMapper.getMappedFields(query.getFieldsObject(), persistentEntity);
				Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), persistentEntity);

				FindPublisher<Document> findPublisher = collection.find(mappedQuery).projection(mappedFields);
				QueryCursorPreparer cursorPreparer = new QueryCursorPreparer(query, entityType);

				final ReadDbObjectCallback<T> readCallback = new ReadDbObjectCallback<T>(mongoConverter, entityType,
						collection.getNamespace().getCollectionName());

				return Flux.from(cursorPreparer.prepare(findPublisher)).map(new Function<Document, T>() {
					@Override
					public T apply(Document document) {
						return readCallback.doWith(document);
					}
				});
			}
		});
	}

	public String getCollectionName(Class<?> entityClass) {
		return this.determineCollectionName(entityClass);
	}

	public Flux<Document> executeCommand(String jsonCommand) {

		Assert.notNull(jsonCommand, "Command must not be empty!");
		return executeCommand(Document.parse(jsonCommand));
	}

	public Flux<Document> executeCommand(final Document command) {

		Assert.notNull(command, "Command must not be null!");

		return createFlux(new ReactiveDbCallback<Document>() {
			@Override
			public Publisher<Document> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.runCommand(command);
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(com.mongodb.Document, com.mongodb.ReadPreference)
	 */
	public Flux<Document> executeCommand(final Document command, final ReadPreference readPreference) {
		Assert.notNull(command, "Command must not be null!");
		return executeCommand(command, readPreference);

	}

	/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(com.mongodb.Document, com.mongodb.ReadPreference)
		 */
	public Flux<Document> executeCommand(final Bson command, final ReadPreference readPreference) {

		Assert.notNull(command, "Command must not be null!");

		return createFlux(new ReactiveDbCallback<Document>() {
			@Override
			public Publisher<Document> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return readPreference != null ? db.runCommand(command, readPreference) : db.runCommand(command);
			}
		});
	}

	protected void logCommandExecutionError(final Document command, CommandResult result) {
		String error = result.getErrorMessage();
		if (error != null) {
			// TODO: DATADOC-204 allow configuration of logging level / throw
			// throw new
			// InvalidDataAccessApiUsageException("Command execution of " +
			// command.toString() + " failed: " + error);
			LOGGER.warn("Command execution of " + command.toString() + " failed: " + error);
		}
	}

	public <T> Flux<T> createFlux(ReactiveDbCallback<T> action) {

		Assert.notNull(action);

		MongoDatabase db = this.getMongoDatabase();
		return Flux.from(action.doInDB(db));
	}

	public <T> Flux<T> createFlux(Class<?> entityClass, ReactiveCollectionCallback<T> callback) {
		return createFlux(determineCollectionName(entityClass), callback);
	}

	public <T> Flux<T> createFlux(final String collectionName, final ReactiveCollectionCallback<T> callback) {
		return Flux.from(createPublisher(collectionName, callback));
	}

	public <T> Mono<T> createMono(final String collectionName, final ReactiveCollectionCallback<T> callback) {
		return Mono.from(createPublisher(collectionName, callback));
	}

	private <T> Publisher<T> createPublisher(final String collectionName, final ReactiveCollectionCallback<T> callback) {

		Assert.notNull(callback);
		Assert.hasText(collectionName);

		Mono<MongoCollection<Document>> collectionPublisher = Mono
				.fromCallable(() -> getAndPrepareCollection(getMongoDatabase(), collectionName));
		return collectionPublisher.flatMap(collection -> callback.doInCollection(collection));
	}

	public <T> Mono<T> createMono(final ReactiveDbCallback<T> callback) {

		Assert.notNull(callback);

		// TODO: What's the protocol? Some actions need to be done before the callback is issued.
		// this style or are there operators?
		Publisher<T> publisher = new Publisher<T>() {

			@Override
			public void subscribe(Subscriber<? super T> s) {
				try {
					Publisher<T> publisher = callback.doInDB(getMongoDatabase());
					publisher.subscribe(s);
				} catch (RuntimeException e) {
					s.onError(potentiallyConvertRuntimeException(e, exceptionTranslator));
				}
			}
		};

		return Mono.from(publisher);
	}

	public <T> T execute(MongoDatabaseCallback<T> action) {

		Assert.notNull(action);

		try {
			MongoDatabase db = this.getMongoDatabase();
			return action.doInDatabase(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	@Override
	public <T> Flux<T> execute(Class<?> entityClass, ReactiveCollectionCallback<T> action) {
		// TODO
		return null;
	}

	@Override
	public <T> Flux<T> execute(ReactiveDbCallback<T> action) {
		return null;
	}

	public <T> Flux<T> execute(String collectionName, ReactiveCollectionCallback<T> callback) {
		Assert.notNull(callback);
		return createFlux(collectionName, callback);
	}

	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass) {
		return createCollection(determineCollectionName(entityClass));
	}

	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass,
			CollectionOptions collectionOptions) {
		return createCollection(determineCollectionName(entityClass), collectionOptions);
	}

	public Mono<MongoCollection<Document>> createCollection(final String collectionName) {
		return doCreateCollection(collectionName, new CreateCollectionOptions());
	}

	public Mono<MongoCollection<Document>> createCollection(final String collectionName,
			final CollectionOptions collectionOptions) {
		return doCreateCollection(collectionName, convertToCreateCollectionOptions(collectionOptions));
	}

	public MongoCollection<Document> getCollection(final String collectionName) {
		return execute(new MongoDatabaseCallback<MongoCollection<Document>>() {
			@Override
			public MongoCollection<Document> doInDatabase(MongoDatabase db) {
				return db.getCollection(collectionName);
			}
		});
	}

	public <T> Mono<Boolean> collectionExists(Class<T> entityClass) {
		return collectionExists(determineCollectionName(entityClass));
	}

	public Mono<Boolean> collectionExists(final String collectionName) {

		return createMono(new ReactiveDbCallback<Boolean>() {
			@Override
			public Publisher<Boolean> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return Stream.from(db.listCollectionNames()).filter(new Predicate<String>() {
					@Override
					public boolean test(String s) {
						return s.equals(collectionName);
					}
				}).map(new Function<String, Boolean>() {
					@Override
					public Boolean apply(String s) {
						return true;
					}
				}).singleOrDefault(new ScalarSupplier<Boolean>() {
					@Override
					public Boolean get() {
						return null;
					}
				});
			}
		});
	}

	public <T> Mono<Void> dropCollection(Class<T> entityClass) {
		return dropCollection(determineCollectionName(entityClass));
	}

	public Mono<Void> dropCollection(final String collectionName) {
		return createMono(new ReactiveDbCallback<Success>() {
			@Override
			public Publisher<Success> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.getCollection(collectionName).drop();
			}
		}).doOnSuccess(new Consumer<Success>() {
			@Override
			public void accept(Success success) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Dropped collection [" + collectionName + "]");
				}
			}
		}).after();
	}

	// Find methods that take a Query to express the query and that return a single object.

	public <T> Mono<T> findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> Mono<T> findOne(Query query, Class<T> entityClass, String collectionName) {
		if (query.getSortObject() == null) {
			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass);
		}

		query.limit(1);
		return find(query, entityClass, collectionName).next();
	}

	public Mono<Boolean> exists(Query query, Class<?> entityClass) {
		return exists(query, entityClass, determineCollectionName(entityClass));
	}

	public Mono<Boolean> exists(Query query, String collectionName) {
		return exists(query, null, collectionName);
	}

	public Mono<Boolean> exists(final Query query, final Class<?> entityClass, String collectionName) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}

		return Stream.from(createFlux(collectionName, new ReactiveCollectionCallback<Document>() {
			@Override
			public Publisher<Document> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {
				Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), getPersistentEntity(entityClass));
				return collection.find(mappedQuery).limit(1);
			}
		})).hasElements();
	}

	// Find methods that take a Query to express the query and that return a List of objects.

	public <T> Flux<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> Flux<T> find(final Query query, Class<T> entityClass, String collectionName) {

		if (query == null) {
			return findAll(entityClass, collectionName);
		}

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new QueryCursorPreparer(query, entityClass));
	}

	public <T> Mono<T> findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, determineCollectionName(entityClass));
	}

	public <T> Mono<T> findById(Object id, Class<T> entityClass, String collectionName) {
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
		MongoPersistentProperty idProperty = persistentEntity == null ? null : persistentEntity.getIdProperty();
		String idKey = idProperty == null ? ID_FIELD : idProperty.getName();
		return doFindOne(collectionName, new Document(idKey, id), null, entityClass);
	}

	/*public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass) {
		return geoNear(near, entityClass, determineCollectionName(entityClass));
	} */

	@SuppressWarnings("unchecked")
	// GeoFlux/GeoPublisher??
	/*public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass, String collectionName) {

		if (near == null) {
			throw new InvalidDataAccessApiUsageException("NearQuery must not be null!");
		}

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException("Entity class must not be null!");
		}

		String collection = StringUtils.hasText(collectionName) ? collectionName : determineCollectionName(entityClass);
		Document nearDbObject = near.toDBObject();

		BasicDBObject command = new BasicDBObject("geoNear", collection);
		command.putAll(nearDbObject);

		if (nearDbObject.containsField("query")) {
			Document query = (Document) nearDbObject.get("query");
			command.put("query", queryMapper.getMappedObject(query, getPersistentEntity(entityClass)));
		}

		CommandResult commandResult = executeCommand(command, this.readPreference);
		List<Object> results = (List<Object>) commandResult.get("results");
		results = results == null ? Collections.emptyList() : results;

		DbObjectCallback<GeoResult<T>> callback = new GeoNearResultDbObjectCallback<T>(
				new ReadDbObjectCallback<T>(mongoConverter, entityClass, collectionName), near.getMetric());
		List<GeoResult<T>> result = new ArrayList<GeoResult<T>>(results.size());

		int index = 0;
		int elementsToSkip = near.getSkip() != null ? near.getSkip() : 0;

		for (Object element : results) {

			/
			 * As MongoDB currently (2.4.4) doesn't support the skipping of elements in near queries
			 * we skip the elements ourselves to avoid at least the document 2 object mapping overhead.
			 *
			 * @see https://jira.mongodb.org/browse/SERVER-3925
			 /
			if (index >= elementsToSkip) {
				result.add(callback.doWith((Document) element));
			}
			index++;
		}

		if (elementsToSkip > 0) {
			// as we skipped some elements we have to calculate the averageDistance ourselves:
			return new GeoResults<T>(result, near.getMetric());
		}

		GeoCommandStatistics stats = GeoCommandStatistics.from(commandResult);
		return new GeoResults<T>(result, new Distance(stats.getAverageDistance(), near.getMetric()));
	}*/

	public <T> Mono<T> findAndModify(Query query, Update update, Class<T> entityClass) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, determineCollectionName(entityClass));
	}

	public <T> Mono<T> findAndModify(Query query, Update update, Class<T> entityClass, String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	public <T> Mono<T> findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass) {
		return findAndModify(query, update, options, entityClass, determineCollectionName(entityClass));
	}

	public <T> Mono<T> findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName) {
		return doFindAndModify(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass, update, options);
	}

	// Find methods that take a Query to express the query and that return a single object that is also removed from the
	// collection in the database.

	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass, String collectionName) {

		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass);
	}

	public Mono<Long> count(Query query, Class<?> entityClass) {
		Assert.notNull(entityClass);
		return count(query, entityClass, determineCollectionName(entityClass));
	}

	public Mono<Long> count(final Query query, String collectionName) {
		return count(query, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public Mono<Long> count(final Query query, final Class<?> entityClass, String collectionName) {

		Assert.hasText(collectionName);

		return createMono(collectionName, new ReactiveCollectionCallback<Long>() {

			@Override
			public Publisher<Long> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				final Document Document = query == null ? null
						: queryMapper.getMappedObject(query.getQueryObject(),
								entityClass == null ? null : mappingContext.getPersistentEntity(entityClass));

				return collection.count(Document);
			}
		});
	}

	@Override
	public Mono<Void> insert(Mono<? extends Object> objectToSave) {
		// TODO
		return null;
	}

	@Override
	public Mono<Void> insert(Publisher<? extends Object> batchToSave, Class<?> entityClass) {
		return null;
	}

	@Override
	public Mono<Void> insert(Publisher<? extends Object> batchToSave, String collectionName) {
		return null;
	}

	@Override
	public Mono<Void> insertAll(Publisher<? extends Object> objectsToSave) {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#insert(java.lang.Object)
	 */
	public Mono<Void> insert(Object objectToSave) {
		ensureNotIterable(objectToSave);
		return insert(objectToSave, determineEntityCollectionName(objectToSave));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#insert(java.lang.Object, java.lang.String)
	 */
	public Mono<Void> insert(Object objectToSave, String collectionName) {
		ensureNotIterable(objectToSave);
		return doInsert(collectionName, objectToSave, this.mongoConverter);
	}

	protected void ensureNotIterable(Object o) {
		if (null != o) {

			boolean isIterable = o.getClass().isArray();

			if (!isIterable) {
				for (Class iterableClass : ITERABLE_CLASSES) {
					if (iterableClass.isAssignableFrom(o.getClass()) || o.getClass().getName().equals(iterableClass.getName())) {
						isIterable = true;
						break;
					}
				}
			}

			if (isIterable) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}

	/**
	 * Prepare the collection before any processing is done using it. This allows a convenient way to apply settings like
	 * slaveOk() etc. Can be overridden in sub-classes.
	 *
	 * @param collection
	 */
	protected MongoCollection<Document> prepareCollection(MongoCollection<Document> collection) {
		if (this.readPreference != null) {
			return collection.withReadPreference(readPreference);
		}
		return collection;
	}

	/**
	 * Prepare the WriteConcern before any processing is done using it. This allows a convenient way to apply custom
	 * settings in sub-classes. <br />
	 * In case of using MongoDB Java driver version 3 the returned {@link WriteConcern} will be defaulted to
	 * {@link WriteConcern#ACKNOWLEDGED} when {@link WriteResultChecking} is set to {@link WriteResultChecking#EXCEPTION}.
	 *
	 * @param mongoAction any WriteConcern already configured or null
	 * @return The prepared WriteConcern or null
	 */
	protected WriteConcern prepareWriteConcern(MongoAction mongoAction) {

		WriteConcern wc = writeConcernResolver.resolve(mongoAction);
		return potentiallyForceAcknowledgedWrite(wc);
	}

	private WriteConcern potentiallyForceAcknowledgedWrite(WriteConcern wc) {

		if (ObjectUtils.nullSafeEquals(WriteResultChecking.EXCEPTION, writeResultChecking)
				&& MongoClientVersion.isMongo3Driver()) {
			if (wc == null || wc.getWObject() == null
					|| (wc.getWObject() instanceof Number && ((Number) wc.getWObject()).intValue() < 1)) {
				return WriteConcern.ACKNOWLEDGED;
			}
		}
		return wc;
	}

	protected <T> Mono<Void> doInsert(String collectionName, T objectToSave, MongoWriter<T> writer) {

		assertUpdateableIdIfNotSet(objectToSave);
		AtomicReference<Document> dbObjectRef = new AtomicReference<>();
		return Mono.just(objectToSave).doOnSuccess(new Consumer<T>() {
			@Override
			public void accept(T t) {
				initializeVersionProperty(objectToSave);
				maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));

			}
		}).map(t -> {
			Document Document = toDbObject(objectToSave, writer);
			dbObjectRef.set(Document);
			return Document;
		}).doOnSuccess(dbDoc -> maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName)))
				.flatMap(dbDoc -> insertDBObject(collectionName, dbDoc, objectToSave.getClass()))
				.doOnNext(new Consumer<Object>() {
					@Override
					public void accept(Object id) {
						populateIdIfNecessary(objectToSave, id);
						maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbObjectRef.get(), collectionName));
					}
				}).after();

	}

	/**
	 * @param objectToSave
	 * @param writer
	 * @return
	 */
	private <T> Document toDbObject(T objectToSave, MongoWriter<T> writer) {

		if (objectToSave instanceof Document) {
			return (Document) objectToSave;
		}

		if (!(objectToSave instanceof String)) {
			Document dbDoc = new Document();
			writer.write(objectToSave, dbDoc);

			if (dbDoc.containsKey(ID_FIELD) && dbDoc.get(ID_FIELD) == null) {
				dbDoc.remove(ID_FIELD);
			}
			return dbDoc;
		} else {
			try {
				return Document.parse((String) objectToSave);
			} catch (JSONParseException e) {
				throw new MappingException("Could not parse given String to save into a JSON document!", e);
			} catch (org.bson.json.JsonParseException e) {
				throw new MappingException("Could not parse given String to save into a JSON document!", e);
			}
		}
	}

	private void initializeVersionProperty(Object entity) {

		MongoPersistentEntity<?> mongoPersistentEntity = getPersistentEntity(entity.getClass());

		if (mongoPersistentEntity != null && mongoPersistentEntity.hasVersionProperty()) {
			ConvertingPropertyAccessor accessor = new ConvertingPropertyAccessor(
					mongoPersistentEntity.getPropertyAccessor(entity), mongoConverter.getConversionService());
			accessor.setProperty(mongoPersistentEntity.getVersionProperty(), 0);
		}
	}

	public Mono<Void> insert(Collection<? extends Object> batchToSave, Class<?> entityClass) {
		return doInsertBatch(determineCollectionName(entityClass), batchToSave, this.mongoConverter);
	}

	public Mono<Void> insert(Collection<? extends Object> batchToSave, String collectionName) {
		return doInsertBatch(collectionName, batchToSave, this.mongoConverter);
	}

	public Mono<Void> insertAll(Collection<? extends Object> objectsToSave) {
		return doInsertAll(objectsToSave, this.mongoConverter);
	}

	protected <T> Mono<Void> doInsertAll(Collection<? extends T> listToSave, MongoWriter<T> writer) {

		final Map<String, List<T>> elementsByCollection = new HashMap<String, List<T>>();

		listToSave.forEach(element -> {
			MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(element.getClass());

			if (entity == null) {
				throw new InvalidDataAccessApiUsageException("No PersistentEntity information found for " + element.getClass());
			}

			String collection = entity.getCollection();
			List<T> collectionElements = elementsByCollection.get(collection);

			if (null == collectionElements) {
				collectionElements = new ArrayList<T>();
				elementsByCollection.put(collection, collectionElements);
			}

			collectionElements.add(element);
		});

		return Flux.fromIterable(elementsByCollection.keySet()).flatMap(collectionName -> {
			return doInsertBatch(collectionName, elementsByCollection.get(collectionName), writer);
		}).after();

	}

	protected <T> Mono<Void> doInsertBatch(final String collectionName, final Collection<? extends T> batchToSave,
			final MongoWriter<T> writer) {

		Assert.notNull(writer);
		Flux<? extends T> batch = Flux.fromIterable(batchToSave);

		Mono<List<Tuple2<T, Document>>> prepareDocuments = Stream.fromIterable(batchToSave)
				.flatMap(new Function<T, Stream<Tuple2<T, Document>>>() {
					@Override
					public Stream<Tuple2<T, Document>> apply(T o) {

						initializeVersionProperty(o);
						maybeEmitEvent(new BeforeConvertEvent<T>(o, collectionName));

						Document dbDoc = toDbObject(o, writer);

						maybeEmitEvent(new BeforeSaveEvent<T>(o, dbDoc, collectionName));
						return Stream.zip(Mono.just(o), Mono.just(dbDoc));
					}
				}).toList();

		Flux<Tuple2<Document, ObjectId>> insertDocuments = prepareDocuments.flatMap(tuples -> {
			List<Document> dbObjects = tuples.stream().map(Tuple2::getT2).collect(Collectors.toList());

			return Stream.zip(Flux.fromIterable(dbObjects), insertDocumentList(collectionName, dbObjects));
		});

		return Flux.zip(batch, insertDocuments).doOnNext(tuple -> {

			populateIdIfNecessary(tuple.getT1(), tuple.getT2().getT2());
			maybeEmitEvent(new AfterSaveEvent<T>(tuple.getT1(), tuple.getT2().getT1(), collectionName));

		}).after();

	}

	static class ObjectDocumentTuple<T> {
		T object;
		Document Document;

		public ObjectDocumentTuple(T object, Document Document) {
			this.object = object;
			this.Document = Document;
		}
	}

	static class ObjectDocumentObjectIdTriplet<T> {
		T object;
		Document Document;
		ObjectId objectId;

		public ObjectDocumentObjectIdTriplet(T object, Document Document, ObjectId objectId) {
			this.object = object;
			this.Document = Document;
			this.objectId = objectId;
		}
	}

	@Override
	public Mono<Void> save(Mono<? extends Object> objectToSave) {

		// TODO
		return null;
	}

	@Override
	public Mono<Void> save(Mono<? extends Object> objectToSave, String collectionName) {
		return null;
	}

	public Mono<Void> save(Object objectToSave) {

		Assert.notNull(objectToSave);
		return save(objectToSave, determineEntityCollectionName(objectToSave));
	}

	public Mono<Void> save(Object objectToSave, String collectionName) {

		Assert.notNull(objectToSave);
		Assert.hasText(collectionName);

		MongoPersistentEntity<?> mongoPersistentEntity = getPersistentEntity(objectToSave.getClass());

		// No optimistic locking -> simple save
		if (mongoPersistentEntity == null || !mongoPersistentEntity.hasVersionProperty()) {
			return doSave(collectionName, objectToSave, this.mongoConverter);
		}

		return doSaveVersioned(objectToSave, mongoPersistentEntity, collectionName);
	}

	private <T> Mono<Void> doSaveVersioned(T objectToSave, MongoPersistentEntity<?> entity, String collectionName) {

		return Mono.just(objectToSave).then(new Function<T, Mono<Void>>() {
			@Override
			public Mono<Void> apply(T o) {
				ConvertingPropertyAccessor convertingAccessor = new ConvertingPropertyAccessor(entity.getPropertyAccessor(o),
						mongoConverter.getConversionService());

				MongoPersistentProperty idProperty = entity.getIdProperty();
				MongoPersistentProperty versionProperty = entity.getVersionProperty();

				Object version = convertingAccessor.getProperty(versionProperty);
				Number versionNumber = convertingAccessor.getProperty(versionProperty, Number.class);

				// Fresh instance -> initialize version property
				if (version == null) {
					return doInsert(collectionName, o, mongoConverter);
				}

				assertUpdateableIdIfNotSet(o);

				// Create query for entity with the id and old version
				Object id = convertingAccessor.getProperty(idProperty);
				Query query = new Query(Criteria.where(idProperty.getName()).is(id).and(versionProperty.getName()).is(version));

				// Bump version number
				convertingAccessor.setProperty(versionProperty, versionNumber.longValue() + 1);

				maybeEmitEvent(new BeforeConvertEvent<T>(o, collectionName));

				Document document = toDbObject(o, mongoConverter);

				maybeEmitEvent(new BeforeSaveEvent<T>(o, document, collectionName));
				Update update = Update.fromDocument(document, ID_FIELD);

				return doUpdate(collectionName, query, update, o.getClass(), false, false).then(updateResult -> {

					maybeEmitEvent(new AfterSaveEvent<T>(o, document, collectionName));
					return Mono.empty();
				});
			}
		});

	}

	protected <T> Mono<Void> doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {

		assertUpdateableIdIfNotSet(objectToSave);
		return Mono.just(objectToSave).then(new Function<T, Mono<Void>>() {
			@Override
			public Mono<Void> apply(T o) {
				maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));

				Document dbDoc = toDbObject(objectToSave, writer);

				maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName));
				return saveDocument(collectionName, dbDoc, objectToSave.getClass())
						.then(new Function<Object, Mono<? extends Void>>() {
							@Override
							public Mono<? extends Void> apply(Object id) {
								populateIdIfNecessary(objectToSave, id);
								maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc, collectionName));
								return Mono.empty();
							}
						});
			}
		});

	}

	protected Mono<Object> insertDBObject(final String collectionName, final Document dbDoc, final Class<?> entityClass) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting Document containing fields: " + dbDoc.keySet() + " in collection: " + collectionName);
		}

		final Document document = new Document(dbDoc);
		Flux<Success> execute = execute(collectionName, new ReactiveCollectionCallback<Success>() {
			public Publisher<Success> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName,
						entityClass, dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

				return collectionToUse.insertOne(document);
			}
		});

		return Stream.from(execute).last().map(success -> document.get(ID_FIELD));
	}

	protected Flux<ObjectId> insertDocumentList(final String collectionName, final List<Document> dbDocList) {
		if (dbDocList.isEmpty()) {
			return Flux.empty();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting list of DBObjects containing " + dbDocList.size() + " items");
		}

		final List<Document> documents = new ArrayList<>();

		return execute(collectionName, new ReactiveCollectionCallback<Success>() {

			@Override
			public Publisher<Success> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT_LIST, collectionName, null,
						null, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

				documents.addAll(toDocuments(dbDocList));

				return collectionToUse.insertMany(documents);
			}
		}).flatMap(new Function<Success, Publisher<? extends Document>>() {
			@Override
			public Publisher<? extends Document> apply(Success s) {
				List<Document> documentsWithIds = documents.stream()
						.filter(document -> document.get(ID_FIELD) instanceof ObjectId).collect(Collectors.toList());
				return Flux.fromIterable(documentsWithIds);
			}
		}).map(document -> document.get(ID_FIELD, ObjectId.class));
	}

	private MongoCollection<Document> prepareCollection(MongoCollection<Document> collection,
			WriteConcern writeConcernToUse) {
		MongoCollection<Document> collectionToUse = collection;

		if (writeConcernToUse != null) {
			collectionToUse = collectionToUse.withWriteConcern(writeConcernToUse);
		}
		return collectionToUse;
	}

	protected Mono<Object> saveDocument(final String collectionName, final Document dbDoc, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Saving Document containing fields: " + dbDoc.keySet());
		}

		return execute(collectionName, new ReactiveCollectionCallback<Object>() {

			@Override
			public Publisher<Object> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
						dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				Publisher<?> publisher;
				if (!dbDoc.containsKey(ID_FIELD)) {
					if (writeConcernToUse == null) {
						publisher = collection.insertOne(dbDoc);
					} else {
						publisher = collection.withWriteConcern(writeConcernToUse).insertOne(dbDoc);
					}
				} else if (writeConcernToUse == null) {
					publisher = collection.replaceOne(Filters.eq(ID_FIELD, dbDoc.get(ID_FIELD)), dbDoc,
							new UpdateOptions().upsert(true));
				} else {
					publisher = collection.withWriteConcern(writeConcernToUse)
							.replaceOne(Filters.eq(ID_FIELD, dbDoc.get(ID_FIELD)), dbDoc, new UpdateOptions().upsert(true));
				}

				return Mono.from(publisher).map(o -> dbDoc.get(ID_FIELD));
			}
		}).next();
	}

	public Mono<UpdateResult> upsert(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	public Mono<UpdateResult> upsert(Query query, Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	public Mono<UpdateResult> upsert(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	public Mono<UpdateResult> updateFirst(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	public Mono<UpdateResult> updateFirst(final Query query, final Update update, final String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	public Mono<UpdateResult> updateFirst(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	public Mono<UpdateResult> updateMulti(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	public Mono<UpdateResult> updateMulti(final Query query, final Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	public Mono<UpdateResult> updateMulti(final Query query, final Update update, Class<?> entityClass,
			String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	protected Mono<UpdateResult> doUpdate(final String collectionName, final Query query, final Update update,
			final Class<?> entityClass, final boolean upsert, final boolean multi) {

		MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);

		Flux<UpdateResult> result = execute(collectionName, new ReactiveCollectionCallback<UpdateResult>() {

			@Override
			public Publisher<UpdateResult> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				increaseVersionForUpdateIfNecessary(entity, update);

				Document queryObj = query == null ? new Document()
						: queryMapper.getMappedObject(query.getQueryObject(), entity);
				Document updateObj = update == null ? new Document()
						: updateMapper.getMappedObject(update.getUpdateObject(), entity);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("Calling update using query: %s and update: %s in collection: %s",
							serializeToJsonSafely(queryObj), serializeToJsonSafely(updateObj), collectionName));
				}

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName,
						entityClass, updateObj, queryObj);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

				UpdateOptions updateOptions = new UpdateOptions().upsert(upsert);

				// TODO hack - split up update and replaces
				boolean useUpdate = false;

				for (String s : updateObj.keySet()) {
					if (s.startsWith("$")) {
						useUpdate = true;
						break;
					}
				}

				collection = writeConcernToUse != null ? collection.withWriteConcern(writeConcernToUse) : collection;

				if (!useUpdate) {
					return collection.replaceOne(queryObj, updateObj, updateOptions);
				}
				if (multi) {
					return collection.updateMany(queryObj, updateObj, updateOptions);
				}
				return collection.updateOne(queryObj, updateObj, updateOptions);

			}
		}).doOnNext(new Consumer<UpdateResult>() {
			@Override
			public void accept(UpdateResult updateResult) {

				if (entity != null && entity.hasVersionProperty() && !multi) {
					if (updateResult.wasAcknowledged() && updateResult.getMatchedCount() == 0) {

						Document queryObj = query == null ? new Document()
								: queryMapper.getMappedObject(query.getQueryObject(), entity);
						Document updateObj = update == null ? new Document()
								: updateMapper.getMappedObject(update.getUpdateObject(), entity);
						if (dbObjectContainsVersionProperty(queryObj, entity))
							throw new OptimisticLockingFailureException("Optimistic lock exception on saving entity: "
									+ updateObj.toString() + " to collection " + collectionName);
					}
				}

			}
		});

		return result.next();
	}

	private void increaseVersionForUpdateIfNecessary(MongoPersistentEntity<?> persistentEntity, Update update) {

		if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
			String versionFieldName = persistentEntity.getVersionProperty().getFieldName();
			if (!update.modifies(versionFieldName)) {
				update.inc(versionFieldName, 1L);
			}
		}
	}

	private boolean dbObjectContainsVersionProperty(Document document, MongoPersistentEntity<?> persistentEntity) {

		if (persistentEntity == null || !persistentEntity.hasVersionProperty()) {
			return false;
		}

		return document.containsKey(persistentEntity.getVersionProperty().getFieldName());
	}

	@Override
	public Mono<DeleteResult> remove(Mono<? extends Object> object) {
		// TODO
		return null;
	}

	@Override
	public Mono<DeleteResult> remove(Mono<? extends Object> object, String collection) {
		return null;
	}

	public Mono<DeleteResult> remove(Object object) {

		if (object == null) {
			return null;
		}

		return remove(getIdQueryFor(object), object.getClass());
	}

	public Mono<DeleteResult> remove(Object object, String collection) {

		Assert.hasText(collection);

		if (object == null) {
			return null;
		}

		return doRemove(collection, getIdQueryFor(object), object.getClass());
	}

	/**
	 * Returns {@link Entry} containing the field name of the id property as {@link Entry#getKey()} and the {@link Id}s
	 * property value as its {@link Entry#getValue()}.
	 *
	 * @param object
	 * @return
	 */
	private Entry<String, Object> extractIdPropertyAndValue(Object object) {

		Assert.notNull(object, "Id cannot be extracted from 'null'.");

		Class<?> objectType = object.getClass();

		if (object instanceof Document) {
			return Collections.singletonMap(ID_FIELD, ((Document) object).get(ID_FIELD)).entrySet().iterator().next();
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(objectType);
		MongoPersistentProperty idProp = entity == null ? null : entity.getIdProperty();

		if (idProp == null || entity == null) {
			throw new MappingException("No id property found for object of type " + objectType);
		}

		Object idValue = entity.getPropertyAccessor(object).getProperty(idProp);
		return Collections.singletonMap(idProp.getFieldName(), idValue).entrySet().iterator().next();
	}

	/**
	 * Returns a {@link Query} for the given entity by its id.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	private Query getIdQueryFor(Object object) {

		Entry<String, Object> id = extractIdPropertyAndValue(object);
		return new Query(where(id.getKey()).is(id.getValue()));
	}

	/**
	 * Returns a {@link Query} for the given entities by their ids.
	 *
	 * @param objects must not be {@literal null} or {@literal empty}.
	 * @return
	 */
	private Query getIdInQueryFor(Collection<?> objects) {

		Assert.notEmpty(objects, "Cannot create Query for empty collection.");

		Iterator<?> it = objects.iterator();
		Entry<String, Object> firstEntry = extractIdPropertyAndValue(it.next());

		ArrayList<Object> ids = new ArrayList<Object>(objects.size());
		ids.add(firstEntry.getValue());

		while (it.hasNext()) {
			ids.add(extractIdPropertyAndValue(it.next()).getValue());
		}

		return new Query(where(firstEntry.getKey()).in(ids));
	}

	private void assertUpdateableIdIfNotSet(Object entity) {

		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entity.getClass());
		MongoPersistentProperty idProperty = persistentEntity == null ? null : persistentEntity.getIdProperty();

		if (idProperty == null || persistentEntity == null) {
			return;
		}

		Object idValue = persistentEntity.getPropertyAccessor(entity).getProperty(idProperty);

		if (idValue == null && !MongoSimpleTypes.AUTOGENERATED_ID_TYPES.contains(idProperty.getType())) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Cannot autogenerate id of type %s for entity of type %s!", idProperty.getType().getName(),
							entity.getClass().getName()));
		}
	}

	public Mono<DeleteResult> remove(Query query, String collectionName) {
		return remove(query, null, collectionName);
	}

	public Mono<DeleteResult> remove(Query query, Class<?> entityClass) {
		return remove(query, entityClass, determineCollectionName(entityClass));
	}

	public Mono<DeleteResult> remove(Query query, Class<?> entityClass, String collectionName) {
		return doRemove(collectionName, query, entityClass);
	}

	protected <T> Mono<DeleteResult> doRemove(final String collectionName, final Query query,
			final Class<T> entityClass) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null!");
		}

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final Document queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);

		return execute(collectionName, new ReactiveCollectionCallback<DeleteResult>() {
			@Override
			public Publisher<DeleteResult> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				maybeEmitEvent(new BeforeDeleteEvent<T>(queryObject, entityClass, collectionName));

				Document dboq = queryMapper.getMappedObject(queryObject, entity);

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName,
						entityClass, null, queryObject);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Remove using query: {} in collection: {}.",
							new Object[] { serializeToJsonSafely(dboq), collectionName });
				}

				return collectionToUse.deleteMany(dboq);
			}
		}).doOnNext(new Consumer<DeleteResult>() {
			@Override
			public void accept(DeleteResult deleteResult) {
				maybeEmitEvent(new AfterDeleteEvent<T>(queryObject, entityClass, collectionName));
			}
		}).next();
	}

	public <T> Flux<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, determineCollectionName(entityClass));
	}

	public <T> Flux<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(null), null,
				new ReadDbObjectCallback<T>(mongoConverter, entityClass, collectionName), collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, String collectionName) {

		return findAllAndRemove(query, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass) {
		return findAllAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName) {
		return doFindAndDelete(collectionName, query, entityClass);
	}

	/**
	 * Retrieve and remove all documents matching the given {@code query} by calling {@link #find(Query, Class, String)}
	 * and {@link #remove(Query, Class, String)}, whereas the {@link Query} for {@link #remove(Query, Class, String)} is
	 * constructed out of the find result.
	 *
	 * @param collectionName
	 * @param query
	 * @param entityClass
	 * @return
	 */
	protected <T> Flux<T> doFindAndDelete(String collectionName, Query query, Class<T> entityClass) {

		Flux<T> flux = find(query, entityClass, collectionName);

		return Stream.from(flux).toList().flatMap(list -> {
			return Stream.from(remove(getIdInQueryFor(list), entityClass, collectionName))
					.flatMap(deleteResult -> Flux.fromIterable(list));
		});
	}

	protected String replaceWithResourceIfNecessary(String function) {

		String func = function;

		if (this.resourceLoader != null && ResourceUtils.isUrl(function)) {

			Resource functionResource = resourceLoader.getResource(func);

			if (!functionResource.exists()) {
				throw new InvalidDataAccessApiUsageException(String.format("Resource %s not found!", function));
			}

			Scanner scanner = null;

			try {
				scanner = new Scanner(functionResource.getInputStream());
				return scanner.useDelimiter("\\A").next();
			} catch (IOException e) {
				throw new InvalidDataAccessApiUsageException(String.format("Cannot read map-reduce file %s!", function), e);
			} finally {
				if (scanner != null) {
					scanner.close();
				}
			}
		}

		return func;
	}

	private void copyMapReduceOptionsToCommand(Query query, MapReduceOptions mapReduceOptions,
			MapReduceCommand mapReduceCommand) {

		if (query != null) {
			if (query.getSkip() != 0 || query.getFieldsObject() != null) {
				throw new InvalidDataAccessApiUsageException(
						"Can not use skip or field specification with map reduce operations");
			}
			if (query.getLimit() > 0 && mapReduceOptions.getLimit() == null) {
				mapReduceCommand.setLimit(query.getLimit());
			}
			// TODO
			if (query.getSortObject() != null) {
				// mapReduceCommand.setSort(queryMapper.getMappedObject(query.getSortObject(), null));
			}
		}

		if (mapReduceOptions.getLimit() != null && mapReduceOptions.getLimit().intValue() > 0) {
			mapReduceCommand.setLimit(mapReduceOptions.getLimit());
		}

		if (mapReduceOptions.getJavaScriptMode() != null) {
			mapReduceCommand.setJsMode(true);
		}
		if (!mapReduceOptions.getExtraOptions().isEmpty()) {
			for (Entry<String, Object> entry : mapReduceOptions.getExtraOptions().entrySet()) {
				ReflectiveMapReduceInvoker.addExtraOption(mapReduceCommand, entry.getKey(), entry.getValue());
			}
		}
		if (mapReduceOptions.getFinalizeFunction() != null) {
			mapReduceCommand.setFinalize(this.replaceWithResourceIfNecessary(mapReduceOptions.getFinalizeFunction()));
		}
		if (mapReduceOptions.getOutputDatabase() != null) {
			mapReduceCommand.setOutputDB(mapReduceOptions.getOutputDatabase());
		}
		if (!mapReduceOptions.getScopeVariables().isEmpty()) {
			mapReduceCommand.setScope(mapReduceOptions.getScopeVariables());
		}
	}

	public Flux<String> getCollectionNames() {
		return createFlux(new ReactiveDbCallback<String>() {
			@Override
			public Publisher<String> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.listCollectionNames();
			}
		});
	}

	public MongoDatabase getMongoDatabase() {
		return mongoDbFactory.getMongoDatabase();
	}

	protected <T> void maybeEmitEvent(MongoMappingEvent<T> event) {
		if (null != eventPublisher) {
			eventPublisher.publishEvent(event);
		}
	}

	/**
	 * Create the specified collection using the provided options
	 *
	 * @param collectionName
	 * @param collectionOptions
	 * @return the collection that was created
	 */
	protected Mono<MongoCollection<Document>> doCreateCollection(final String collectionName,
			final CreateCollectionOptions collectionOptions) {

		return createMono(new ReactiveDbCallback<Success>() {
			@Override
			public Publisher<Success> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.createCollection(collectionName, collectionOptions);
			}
		}).map(new Function<Success, MongoCollection<Document>>() {
			@Override
			public MongoCollection<Document> apply(Success success) {
				// TODO: Emit a collection created event
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Created collection [{}]", collectionName);
				}
				return getCollection(collectionName);
			}
		});
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The query document is specified as a standard {@link Document} and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @return the {@link List} of converted objects.
	 */
	protected <T> Mono<T> doFindOne(String collectionName, Document query, Document fields, Class<T> entityClass) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);
		Document mappedFields = fields == null ? null : queryMapper.getMappedObject(fields, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findOne using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), mappedFields, entityClass, collectionName));
		}

		return executeFindOneInternal(new FindOneCallback(mappedQuery, mappedFields),
				new ReadDbObjectCallback<T>(this.mongoConverter, entityClass, collectionName), collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List using the template's converter. The
	 * query document is specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param fields the document that specifies the fields to be returned
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass) {
		return doFind(collectionName, query, fields, entityClass, null,
				new ReadDbObjectCallback<T>(this.mongoConverter, entityClass, collectionName));
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type. The object is
	 * converted from the MongoDB native representation using an instance of {@see MongoConverter}. The query document is
	 * specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @param preparer allows for customization of the {@link DBCursor} used when iterating over the result set, (apply
	 *          limits, skips and so on).
	 * @return the {@link List} of converted objects.
	 */
	protected <T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass,
			ReactiveCursorPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer,
				new ReadDbObjectCallback<T>(mongoConverter, entityClass, collectionName));
	}

	protected <S, T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<S> entityClass,
			ReactiveCursorPreparer preparer, DbObjectCallback<T> objectCallback) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		Document mappedFields = queryMapper.getMappedFields(fields, entity);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("find using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(mappedQuery), mappedFields, entityClass, collectionName));
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields), preparer, objectCallback,
				collectionName);
	}

	protected CreateCollectionOptions convertToCreateCollectionOptions(CollectionOptions collectionOptions) {
		CreateCollectionOptions result = new CreateCollectionOptions();
		if (collectionOptions != null) {
			if (collectionOptions.getCapped() != null) {
				result = result.capped(collectionOptions.getCapped().booleanValue());
			}
			if (collectionOptions.getSize() != null) {
				result = result.sizeInBytes(collectionOptions.getSize().intValue());
			}
			if (collectionOptions.getMaxDocuments() != null) {
				result = result.maxDocuments(collectionOptions.getMaxDocuments().intValue());
			}
		}
		return result;
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The first document that matches the query is returned and also removed from the collection in the database.
	 * <p/>
	 * The query document is specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> Mono<T> doFindAndRemove(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass) {
		EntityReader<? super T, Bson> readerToUse = this.mongoConverter;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findAndRemove using query: %s fields: %s sort: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), fields, sort, entityClass, collectionName));
		}
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		return executeFindOneInternal(new FindAndRemoveCallback(queryMapper.getMappedObject(query, entity), fields, sort),
				new ReadDbObjectCallback<T>(readerToUse, entityClass, collectionName), collectionName);
	}

	protected <T> Mono<T> doFindAndModify(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass, Update update, FindAndModifyOptions options) {

		EntityReader<? super T, Bson> readerToUse = this.mongoConverter;

		if (options == null) {
			options = new FindAndModifyOptions();
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		increaseVersionForUpdateIfNecessary(entity, update);

		Document mappedQuery = queryMapper.getMappedObject(query, entity);
		Document mappedUpdate = updateMapper.getMappedObject(update.getUpdateObject(), entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER
					.debug(
							String.format(
									"findAndModify using query: %s fields: %s sort: %s for class: %s and update: %s "
											+ "in collection: %s",
									serializeToJsonSafely(mappedQuery), fields, sort, entityClass, serializeToJsonSafely(mappedUpdate),
									collectionName));
		}

		return executeFindOneInternal(new FindAndModifyCallback(mappedQuery, fields, sort, mappedUpdate, options),
				new ReadDbObjectCallback<T>(readerToUse, entityClass, collectionName), collectionName);
	}

	/**
	 * Populates the id property of the saved object, if it's not set already.
	 *
	 * @param savedObject
	 * @param id
	 */
	protected void populateIdIfNecessary(Object savedObject, Object id) {

		if (id == null) {
			return;
		}

		if (savedObject instanceof BasicDBObject) {
			Document Document = (Document) savedObject;
			Document.put(ID_FIELD, id);
			return;
		}

		MongoPersistentProperty idProp = getIdPropertyFor(savedObject.getClass());

		if (idProp == null) {
			return;
		}

		ConversionService conversionService = mongoConverter.getConversionService();
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(savedObject.getClass());
		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(savedObject);

		if (accessor.getProperty(idProp) != null) {
			return;
		}

		new ConvertingPropertyAccessor(accessor, conversionService).setProperty(idProp, id);
	}

	private DBCollection getAndPrepareCollection(DB db, String collectionName) {
		try {
			DBCollection collection = db.getCollection(collectionName);
			return collection;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	private MongoCollection<Document> getAndPrepareCollection(MongoDatabase db, String collectionName) {
		try {
			MongoCollection<Document> collection = db.getCollection(collectionName);
			return prepareCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/**
	 * Internal method using callbacks to do queries against the datastore that requires reading a single object from a
	 * collection of objects. It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ConnectionCallback} for a {@link Document}.</li>
	 * <li>Apply the given {@link DbObjectCallback} to each of the {@link Document}s to obtain the result.</li>
	 * <ol>
	 *
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link Document} with
	 * @param objectCallback the {@link DbObjectCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> Mono<T> executeFindOneInternal(ReactiveCollectionCallback<Document> collectionCallback,
			final DbObjectCallback<T> objectCallback, String collectionName) {

		try {

			Publisher<Document> publisher = collectionCallback
					.doInCollection(getAndPrepareCollection(getMongoDatabase(), collectionName));
			return Mono.from(publisher).map(new Function<Document, T>() {
				@Override
				public T apply(Document document) {
					return objectCallback.doWith(document);
				}
			});

		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/**
	 * Internal method using callback to do queries against the datastore that requires reading a collection of objects.
	 * It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ConnectionCallback} for a {@link DBCursor}.</li>
	 * <li>Prepare that {@link DBCursor} with the given {@link CursorPreparer} (will be skipped if {@link CursorPreparer}
	 * is {@literal null}</li>
	 * <li>Iterate over the {@link DBCursor} and applies the given {@link DbObjectCallback} to each of the
	 * {@link Document}s collecting the actual result {@link List}.</li>
	 * <ol>
	 *
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link DBCursor} with
	 * @param preparer the {@link CursorPreparer} to potentially modify the {@link DBCursor} before ireating over it
	 * @param objectCallback the {@link DbObjectCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> Flux<T> executeFindMultiInternal(MongoCollectionCallback<FindPublisher<Document>> collectionCallback,
			ReactiveCursorPreparer preparer, final DbObjectCallback<T> objectCallback, String collectionName) {

		try {

			FindPublisher<Document> cursor = collectionCallback
					.doInCollection(getAndPrepareCollection(getMongoDatabase(), collectionName));

			if (preparer != null) {
				cursor = preparer.prepare(cursor);
			}

			return Flux.from(cursor).map(new Function<Document, T>() {
				@Override
				public T apply(Document document) {
					return objectCallback.doWith(document);
				}
			});
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	private MongoPersistentEntity<?> getPersistentEntity(Class<?> type) {
		return type == null ? null : mappingContext.getPersistentEntity(type);
	}

	private MongoPersistentProperty getIdPropertyFor(Class<?> type) {
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(type);
		return persistentEntity == null ? null : persistentEntity.getIdProperty();
	}

	private <T> String determineEntityCollectionName(T obj) {
		if (null != obj) {
			return determineCollectionName(obj.getClass());
		}

		return null;
	}

	String determineCollectionName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity collection can't be determined!");
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException(
					"No Persistent Entity information found for the class " + entityClass.getName());
		}
		return entity.getCollection();
	}

	/**
	 * Handles {@link WriteResult} errors based on the configured {@link WriteResultChecking}.
	 *
	 * @param writeResult
	 * @param query
	 * @param operation
	 */
	protected void handleAnyWriteResultErrors(WriteResult writeResult, Document query, MongoActionOperation operation) {

		if (writeResultChecking == WriteResultChecking.NONE) {
			return;
		}

		String error = ReflectiveWriteResultInvoker.getError(writeResult);

		if (error == null) {
			return;
		}

		String message;

		switch (operation) {

			case INSERT:
			case SAVE:
				message = String.format("Insert/Save for %s failed: %s", query, error);
				break;
			case INSERT_LIST:
				message = String.format("Insert list failed: %s", error);
				break;
			default:
				message = String.format("Execution of %s%s failed: %s", operation,
						query == null ? "" : " using query " + query.toString(), error);
		}

		if (writeResultChecking == WriteResultChecking.EXCEPTION) {
			throw new MongoDataIntegrityViolationException(message, writeResult, operation);
		} else {
			LOGGER.error(message);
			return;
		}
	}

	/**
	 * Inspects the given {@link CommandResult} for erros and potentially throws an
	 * {@link InvalidDataAccessApiUsageException} for that error.
	 *
	 * @param result must not be {@literal null}.
	 * @param source must not be {@literal null}.
	 */
	private void handleCommandError(CommandResult result, Document source) {

		try {
			result.throwOnError();
		} catch (MongoException ex) {

			String error = result.getErrorMessage();
			error = error == null ? "NO MESSAGE" : error;

			throw new InvalidDataAccessApiUsageException(
					"Command execution failed:  Error [" + error + "], Command = " + source, ex);
		}
	}

	private static final MappingMongoConverter getDefaultMongoConverter(ReactiveMongoDbFactory factory) {

		MappingMongoConverter converter = new MappingMongoConverter(new NoOpDbRefResolver(), new MongoMappingContext());
		converter.afterPropertiesSet();
		return converter;
	}

	private Document getMappedSortObject(Query query, Class<?> type) {

		if (query == null || query.getSortObject() == null) {
			return null;
		}

		return queryMapper.getMappedSort(query.getSortObject(), mappingContext.getPersistentEntity(type));
	}

	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe re-throwing of the return value.
	 *
	 * @param ex the exception to translate
	 * @param exceptionTranslator the {@link PersistenceExceptionTranslator} to be used for translation
	 * @return
	 */
	private static RuntimeException potentiallyConvertRuntimeException(RuntimeException ex,
			PersistenceExceptionTranslator exceptionTranslator) {
		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	// Callback implementations

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 */
	private static class FindOneCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Document fields;

		public FindOneCallback(Document query, Document fields) {
			this.query = query;
			this.fields = fields;
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {
			if (fields == null) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("findOne using query: %s in db.collection: %s", serializeToJsonSafely(query),
							collection.getNamespace().getFullName()));
				}
				return collection.find(query).projection(fields).limit(1).first();
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("findOne using query: %s fields: %s in db.collection: %s",
							serializeToJsonSafely(query), fields, collection.getNamespace().getFullName()));
				}
				return collection.find(query).projection(fields).limit(1);
			}
		}
	}

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 */
	private static class FindCallback implements MongoCollectionCallback<FindPublisher<Document>> {

		private final Document query;
		private final Document fields;

		public FindCallback(Document query) {
			this(query, null);
		}

		public FindCallback(Document query, Document fields) {
			this.query = query;
			this.fields = fields;
		}

		@Override
		public FindPublisher<Document> doInCollection(MongoCollection<Document> collection) {

			FindPublisher<Document> findPublisher;
			if (query == null || query.isEmpty()) {
				findPublisher = collection.find();
			} else {
				findPublisher = collection.find(query);
			}

			if (fields == null || fields.isEmpty()) {
				return findPublisher;
			} else {
				return findPublisher.projection(fields);
			}
		}
	}

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Thomas Risberg
	 */
	private static class FindAndRemoveCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;

		public FindAndRemoveCallback(Document query, Document fields, Document sort) {
			this.query = query;
			this.fields = fields;
			this.sort = sort;
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {
			FindOneAndDeleteOptions findOneAndDeleteOptions = convertToFindOneAndDeleteOptions(fields, sort);
			return collection.findOneAndDelete(query, findOneAndDeleteOptions);
		}
	}

	private static class FindAndModifyCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Document update;
		private final FindAndModifyOptions options;

		public FindAndModifyCallback(Document query, Document fields, Document sort, Document update,
				FindAndModifyOptions options) {
			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			if (options.isRemove()) {
				FindOneAndDeleteOptions findOneAndDeleteOptions = convertToFindOneAndDeleteOptions(fields, sort);
				return collection.findOneAndDelete(query, findOneAndDeleteOptions);
			}

			FindOneAndUpdateOptions findOneAndUpdateOptions = convertToFindOneAndUpdateOptions(options, fields, sort);
			return collection.findOneAndUpdate(query, update, findOneAndUpdateOptions);
		}

		private FindOneAndUpdateOptions convertToFindOneAndUpdateOptions(FindAndModifyOptions options, Document fields,
				Document sort) {

			FindOneAndUpdateOptions result = new FindOneAndUpdateOptions();

			result = result.projection(fields).sort(sort).upsert(options.isUpsert());

			if (options.isReturnNew()) {
				result = result.returnDocument(ReturnDocument.AFTER);
			} else {
				result = result.returnDocument(ReturnDocument.BEFORE);
			}

			return result;
		}

	}

	private static FindOneAndDeleteOptions convertToFindOneAndDeleteOptions(Document fields, Document sort) {

		FindOneAndDeleteOptions result = new FindOneAndDeleteOptions();
		result = result.projection(fields).sort(sort);

		return result;
	}

	/**
	 * Simple internal callback to allow operations on a {@link Document}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */

	static interface DbObjectCallback<T> {

		T doWith(Document object);
	}

	/**
	 * Simple internal callback to allow operations on a {@link MongoDatabase}.
	 *
	 * @author Mark Paluch
	 */

	static interface MongoDatabaseCallback<T> {

		T doInDatabase(MongoDatabase db);
	}

	/**
	 * Simple internal callback to allow operations on a {@link MongoCollection}.
	 *
	 * @author Mark Paluch
	 */

	static interface MongoCollectionCallback<T> {

		T doInCollection(MongoCollection<Document> collection);
	}

	/**
	 * Simple {@link DbObjectCallback} that will transform {@link Document} into the given target type using the given
	 * {@link EntityReader}.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	private class ReadDbObjectCallback<T> implements DbObjectCallback<T> {

		private final EntityReader<? super T, Bson> reader;
		private final Class<T> type;
		private final String collectionName;

		public ReadDbObjectCallback(EntityReader<? super T, Bson> reader, Class<T> type, String collectionName) {

			Assert.notNull(reader);
			Assert.notNull(type);
			this.reader = reader;
			this.type = type;
			this.collectionName = collectionName;
		}

		public T doWith(Document object) {
			if (null != object) {
				// TODO
				// maybeEmitEvent(new AfterLoadEvent<T>(object, type, collectionName));
			}
			T source = reader.read(type, object);
			if (null != source) {
				// maybeEmitEvent(new AfterConvertEvent<T>(object, source, collectionName));
			}
			return source;
		}
	}

	class UnwrapAndReadDbObjectCallback<T> extends ReadDbObjectCallback<T> {

		public UnwrapAndReadDbObjectCallback(MappingMongoConverter reader, Class<T> type, String collectionName) {
			super(reader, type, collectionName);
		}

		@Override
		public T doWith(Document object) {

			Object idField = object.get(Fields.UNDERSCORE_ID);

			if (!(idField instanceof Document)) {
				return super.doWith(object);
			}

			Document toMap = new Document();
			Document nested = (Document) idField;
			toMap.putAll(nested);

			for (String key : object.keySet()) {
				if (!Fields.UNDERSCORE_ID.equals(key)) {
					toMap.put(key, object.get(key));
				}
			}

			return super.doWith(toMap);
		}
	}

	class QueryCursorPreparer implements ReactiveCursorPreparer {

		private final Query query;
		private final Class<?> type;

		public QueryCursorPreparer(Query query, Class<?> type) {

			this.query = query;
			this.type = type;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.CursorPreparer#prepare(com.mongodb.DBCursor)
		 */
		public <T> FindPublisher<T> prepare(FindPublisher<T> cursor) {

			if (query == null) {
				return cursor;
			}

			if (query.getSkip() <= 0 && query.getLimit() <= 0 && query.getSortObject() == null
					&& !StringUtils.hasText(query.getHint()) && !query.getMeta().hasValues()) {
				return cursor;
			}

			FindPublisher<T> cursorToUse = cursor;

			try {
				if (query.getSkip() > 0) {
					cursorToUse = cursorToUse.skip(query.getSkip());
				}
				if (query.getLimit() > 0) {
					cursorToUse = cursorToUse.limit(query.getLimit());
				}
				if (query.getSortObject() != null) {
					Document sortDbo = type != null ? getMappedSortObject(query, type) : query.getSortObject();
					cursorToUse = cursorToUse.sort(sortDbo);
				}
				BasicDBObject modifiers = new BasicDBObject();

				if (StringUtils.hasText(query.getHint())) {
					modifiers.append("$hint", query.getHint());
				}

				if (query.getMeta().hasValues()) {
					for (Entry<String, Object> entry : query.getMeta().values()) {
						modifiers.append(entry.getKey(), entry.getValue());
					}
				}

				if (!modifiers.isEmpty()) {
					cursorToUse = cursorToUse.modifiers(modifiers);
				}

			} catch (RuntimeException e) {
				throw potentiallyConvertRuntimeException(e, exceptionTranslator);
			}

			return cursorToUse;
		}
	}

	private static List<? extends Document> toDocuments(final Collection<? extends Document> documents) {
		return new ArrayList<>(documents);
	}

	static class NoOpDbRefResolver implements DbRefResolver {

		@Override
		public Object resolveDbRef(MongoPersistentProperty property, DBRef dbref, DbRefResolverCallback callback,
				DbRefProxyHandler proxyHandler) {
			return null;
		}

		@Override
		public DBRef createDbRef(org.springframework.data.mongodb.core.mapping.DBRef annotation,
				MongoPersistentEntity<?> entity, Object id) {
			return null;
		}

		@Override
		public Document fetch(DBRef dbRef) {
			return null;
		}
	}

}
