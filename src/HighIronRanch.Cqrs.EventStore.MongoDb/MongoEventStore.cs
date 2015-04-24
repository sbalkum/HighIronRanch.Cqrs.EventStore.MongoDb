using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;
using SimpleCqrs.Eventing;

namespace HighIronRanch.Cqrs.EventStore.MongoDb
{
	public class MongoEventStore : IEventStore
	{
		private readonly string _connectionString;
		private readonly string _database;

		public MongoEventStore(IMongoEventStoreSettings settings)
		{
			_connectionString = settings.MongoDbEventStoreConnectionString;
			_database = settings.MongoDbEventStoreDatabase;

			BsonClassMap.RegisterClassMap<DomainEvent>();

			var assemblies = AppDomain.CurrentDomain.GetAssemblies();
			var domainEventSubclasses = new List<Type>();
			foreach (var a in assemblies)
			{
				var types = a.GetTypes().Where(t => typeof (DomainEvent).IsAssignableFrom(t));
				domainEventSubclasses.AddRange(types);
			}

			foreach (var subclass in domainEventSubclasses)
			{
				BsonClassMap.LookupClassMap(subclass);
			}
		}

		protected MongoDatabase GetDatabase()
		{
			var client = new MongoClient(_connectionString);
			return client.GetServer().GetDatabase(_database);
		}

		protected MongoCollection<DomainEvent> GetEventsCollection()
		{
			return GetDatabase().GetCollection<DomainEvent>("events");
		}

		public IEnumerable<DomainEvent> GetEvents(Guid aggregateRootId, int startSequence)
		{
			return IOExceptionRetriable.Run(() => GetEventsCollection()
				.AsQueryable<DomainEvent>()
				.Where(e => e.AggregateRootId == aggregateRootId)
				.Where(e => e.Sequence >= startSequence)
				.ToList());
		}

		public void Insert(IEnumerable<DomainEvent> domainEvents)
		{
			if (domainEvents.Any())
			{
				IOExceptionRetriable.Run(() => GetEventsCollection().InsertBatch(domainEvents));
			}
		}

		public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes)
		{
			var array = domainEventTypes.Select(t => t.Name);
			var selector = Query.In("_t", new BsonArray(array));

			return IOExceptionRetriable.Run(() => GetEventsCollection().Find(selector));
		}

		public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, Guid aggregateRootId)
		{
			var selector = Query.And(Query.In("_t", new BsonArray(domainEventTypes.Select(t => t.Name))),
									Query.EQ("AggregateRootId", aggregateRootId));

			return IOExceptionRetriable.Run(() => GetEventsCollection().Find(selector));
		}

		public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, DateTime startDate, DateTime endDate)
		{
			var selector = Query.And(Query.In("_t", new BsonArray(domainEventTypes.Select(t => t.Name))),
									Query.GTE("EventDate", startDate),
									Query.LTE("EventDate", endDate));

			return IOExceptionRetriable.Run(() => GetEventsCollection().Find(selector));
		}

		public void DeleteEventsByEventType(IEnumerable<Type> domainEventTypes)
		{
			var selector = Query.In("_t", new BsonArray(domainEventTypes.Select(t => t.Name)));

			IOExceptionRetriable.Run(() => GetEventsCollection().Remove(selector));
		}
	}
}
