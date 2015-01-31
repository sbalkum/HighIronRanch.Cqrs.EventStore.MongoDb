using System;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;
using SimpleCqrs;
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

		public Snapshot GetSnapshot(Guid aggregateRootId)
		{
			var database = GetDatabase();
			var snapshotsCollection = database.GetCollection<Snapshot>("snapshots");
			return (from snapshot in ((MongoCollection)snapshotsCollection).AsQueryable<Snapshot>()
				where snapshot.AggregateRootId == aggregateRootId
				select snapshot).SingleOrDefault();
		}

		public void SaveSnapshot<TSnapshot>(TSnapshot snapshot) where TSnapshot : Snapshot
		{
			var database = GetDatabase();
			var snapshotsCollection = database.GetCollection<Snapshot>("snapshots");
			var query = Query.EQ("_id", snapshot.AggregateRootId);
			snapshotsCollection.Update(query, Update.Replace(snapshot), UpdateFlags.Upsert);
		}
	}
}