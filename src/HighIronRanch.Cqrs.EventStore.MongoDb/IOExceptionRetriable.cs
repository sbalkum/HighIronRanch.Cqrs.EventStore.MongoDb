using System;
using System.IO;

namespace HighIronRanch.Cqrs.EventStore.MongoDb
{
	internal static class IOExceptionRetriable
	{
		private const int MaxNumberOfAttempts = 3;

		public static T Run<T>(Func<T> action)
		{
			for (int i = 1; i <= MaxNumberOfAttempts; i++)
			{
				try
				{
					return action();
				}
				catch (IOException)
				{
					if (i == MaxNumberOfAttempts)
						throw;
				}
			}
			throw new Exception("Something went wrong after " + MaxNumberOfAttempts + " attempts");
		}
	}
}