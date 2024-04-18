using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetEnv;
using MongoDB.Driver;
using System.IO;
using CsvHelper;
using System.Globalization;
using Polly;
using System.Diagnostics;
using System.Threading;
using Polly.Retry;
using Serilog;
using Serilog.Sinks.File;
using Serilog.Sinks.SystemConsole;
using System.Net.Http;

namespace ffxiv
{
	class Program
	{
		static IMongoClient DBClient;
		private static CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
		
		static async Task Main(string[] args)
		{
			Log.Logger = new LoggerConfiguration().WriteTo.Console().WriteTo.File(Path.GetFullPath("./logs/log.txt", Environment.CurrentDirectory)).CreateLogger();

			Log.Information("Starting program.");

			Env.TraversePath().Load();
			
			APIClient ApiClient = new();
			RetryStrategyOptions retryStrategy = new()
			{
				MaxRetryAttempts = 5,
				UseJitter = true,
				BackoffType = DelayBackoffType.Linear,
				OnRetry = static args =>
				{
					Log.Warning("API call failed with error {0}, retrying: {1}", args.Outcome.Exception.Message, args.AttemptNumber);
					return default;
				},
				ShouldHandle = new PredicateBuilder().Handle<HttpRequestException>()
			};

			ResiliencePipeline pipeline = new ResiliencePipelineBuilder()
			.AddRetry(retryStrategy) 
			.AddTimeout(TimeSpan.FromSeconds(10))
			.Build();
			
			// add null check before inserting to DB
			// potentially add failed api calls to retry after the rest have finished


			try
			{
				DBClient = new MongoClient(Environment.GetEnvironmentVariable("DBCONNECTIONSTRING"));
				Log.Information("Successfully connected to DB");
			}
			catch (Exception e)
			{
				Log.Fatal("Failed to connect to MongoDB: " + e.Message);
				await Log.CloseAndFlushAsync();
				return;
			}

			//load list of valid item IDs
			List<List<string>> toCall = new();
			using (var reader = new StreamReader("../../../ItemIDs.csv"))
			using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
			{
				List<CSVItem> ids = csv.GetRecords<CSVItem>().ToList();
				List<string> tempIds = new();
				foreach (CSVItem id in ids)
				{
					if (tempIds.Count() >= 100)
					{
						toCall.Add(tempIds);
						tempIds = new();
					}
					tempIds.Add(id.Id);

				}
				if (tempIds.Any())
				{
					toCall.Add(tempIds);
				}
			}
			//toCall.Clear();
			//toCall.Add(new List<string>() { "28752", "28753", "28754", "28755", "28756", "28757", "28758", "28759", "28760", "28761", "28762", "28763", "28764", "28765", "28766", "28767", "28768", "28769", "28770", "28771", "28772", "28773", "28774", "28775", "28776", "28777", "28778", "28779", "28780", "28781", "28782", "28783", "28784", "28785", "28786", "28787", "28788", "28789", "28790", "28791", "28792", "28793", "28794", "28795", "28796", "28797", "28798", "28799", "28800", "28801", "28802", "28803", "28804", "28805", "28806", "28807", "28808", "28809", "28810", "28811", "28812", "28813", "28814", "28815", "28816", "28817", "28818", "28819", "28820", "28821", "28822", "28823", "28824", "28825", "28826", "28827", "28828", "28829", "28830", "28831", "28832", "28833", "28834", "28835", "28836", "28837", "28838", "28839", "28840", "28841", "28842", "28843", "28844", "28845", "28846", "28847", "28848", "28849", "28850", "28851"});
			foreach (List<string> ids in toCall)
			{
				try
				{
					Log.Information("Calling API");

					APIResponse AetherRes = await pipeline.ExecuteAsync(
						async token => await ApiClient.CallAPI("Aether", ids),
						cancellationTokenSource.Token);
					APIResponse CrystalRes = await pipeline.ExecuteAsync(
						async token => await ApiClient.CallAPI("Crystal", ids),
						cancellationTokenSource.Token);
					APIResponse DynamisRes = await pipeline.ExecuteAsync(
						async token => await ApiClient.CallAPI("Dynamis", ids),
						cancellationTokenSource.Token); 
					APIResponse PrimalRes = await pipeline.ExecuteAsync(
						async token => await ApiClient.CallAPI("Primal", ids),
						cancellationTokenSource.Token);

					Log.Information("Finished calling API, Upserting into DB");

					bool AetherStatus = await InsertToDB("NA", AetherRes);
					bool CrystalStatus = await InsertToDB("NA", CrystalRes);
					bool DynamisStatus = await InsertToDB("NA", DynamisRes);
					bool PrimalStatus = await InsertToDB("NA", PrimalRes);

					//if one or more inserts fails, break out of loop
					if (!AetherStatus)
					{
						Log.Error("Aether insert/update failed.");
					}
					if (!CrystalStatus)
					{
						Log.Error("Crystal insert/update failed.");
					}
					if (!DynamisStatus)
					{
						Log.Error("Dynamis insert/update failed.");
					}
					if (!PrimalStatus)
					{
						Log.Error("Primal insert/update failed.");
					}
					if (!AetherStatus || !CrystalStatus || !DynamisStatus || !PrimalStatus)
					{
						break;
					}
					Log.Information("Finished upserting batch {0}-{1}", ids[0], ids[^1]);
				}
				catch (Exception e)
				{
					Log.Error(e.Message);
				}

			}
			Log.Information("Finished inserting " + toCall.Count + " batches into DB!");
			await Log.CloseAndFlushAsync();

		}
		
		/// <summary>
		/// Attempts to insert/update Items into the database
		/// </summary>
		/// <param name="dbName"> Name of Database, ie name of region</param>
		/// <param name="apiRes"> API response object, </param>
		/// <returns>boolean, true if no errors, false otherwise</returns>
		static async Task<bool> InsertToDB(string dbName, APIResponse apiRes)
		{
			if (apiRes == null)
			{
				throw new Exception("No APIRes object passed to DB for insertion");
			}

			IMongoCollection<Item> ItemCollection = DBClient.GetDatabase(dbName).GetCollection<Item>(apiRes.dcName);
			using (var session = await DBClient.StartSessionAsync())
			{
				// Begin transaction
				session.StartTransaction();

				try
				{
					ReplaceOptions options = new ReplaceOptions();
					options.IsUpsert = true;
					foreach (Item item in apiRes.items)
					{
						ItemCollection.ReplaceOne(Builders<Item>.Filter.Where(i => i.Id == item.Id), item, options);
					}
					await session.CommitTransactionAsync();
				}
				catch
				{
					await session.AbortTransactionAsync();
					throw;
				}

			}

			Log.Information($"Successfully inserted/updated {apiRes.items.Count} items into {dbName} database");

			return true;
		}


	}
	
}
