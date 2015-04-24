using System;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;
using SimpleCqrs.Domain;
using SimpleCqrs.Eventing;

namespace HighIronRanch.Cqrs.EventStore.MongoDb
{
	public class MongoSnapshotStore : ISnapshotStore
	{
		private readonly string _connectionString;
		private readonly string _database;

		public MongoSnapshotStore(IMongoEventStoreSettings settings)
		{
			_connectionString = settings.MongoDbEventStoreConnectionString;
			_database = settings.MongoDbEventStoreDatabase;
		}

		protected MongoDatabase GetDatabase()
		{
			var client = new MongoClient(_connectionString);
			return client.GetServer().GetDatabase(_database);
		}

		protected MongoCollection<Snapshot> GetSnapShotCollection()
		{
			return GetDatabase().GetCollection<Snapshot>("snapshots");
		}

		public Snapshot GetSnapshot(Guid aggregateRootId)
		{
			return IOExceptionRetriable.Run(() => 
				GetSnapShotCollection()
					.AsQueryable<Snapshot>()
					.SingleOrDefault(s => s.AggregateRootId == aggregateRootId)
			);
		}

		public void SaveSnapshot<TSnapshot>(TSnapshot snapshot) where TSnapshot : Snapshot
		{
			var query = Query.EQ("_id", snapshot.AggregateRootId);

			IOExceptionRetriable.Run(() => 
				GetSnapShotCollection()
					.Update(query, Update.Replace(snapshot), UpdateFlags.Upsert)
			);
		}
	}
}