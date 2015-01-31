namespace HighIronRanch.Cqrs.EventStore.MongoDb
{
	public interface IMongoEventStoreSettings
	{
		string MongoDbEventStoreConnectionString { get; }
		string MongoDbEventStoreDatabase { get; }
	}
}