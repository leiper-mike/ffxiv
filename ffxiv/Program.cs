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
using Polly.Extensions.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Diagnostics;

namespace ffxiv
{
	class Program
	{
		static IMongoClient DBClient;
		private static TraceSource ts = new TraceSource("TraceSource");
		static async Task Main(string[] args)
		{ 

			Env.TraversePath().Load();
			
			HostApplicationBuilder HTTPBuilder = Host.CreateApplicationBuilder(args);

			var retryPolicy = HttpPolicyExtensions.HandleTransientHttpError().RetryAsync(3);


			IHttpClientBuilder httpClientBuilder = HTTPBuilder.Services.AddHttpClient<APIClient>(
			configureClient: client =>
			{
				client.BaseAddress = new Uri("https://universalis.app/api/v2/");

			});
			httpClientBuilder.AddPolicyHandler(retryPolicy);

			IHost host = HTTPBuilder.Build();

			APIClient ApiClient = host.Services.GetRequiredService<APIClient>();


			try
			{
				DBClient = new MongoClient(Environment.GetEnvironmentVariable("DBCONNECTIONSTRING"));
				ts.TraceEvent(TraceEventType.Information, 1, "Successfully connected to DB");
			}
			catch (Exception e)
			{
				ts.TraceEvent(TraceEventType.Error, 1, "Failed to connect to MongoDB: " + e.Message);
				ts.Flush();
				return;
			}
			List<List<string>> toCall = new List<List<string>>();
			using (var reader = new StreamReader("../../ItemIDs.csv"))
			using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
			{
				List<CSVItem> ids = csv.GetRecords<CSVItem>().ToList();
				List<string> tempIds = new List<string>();
				foreach (CSVItem id in ids)
				{
					if (tempIds.Count() >= 100)
					{
						toCall.Add(tempIds);
						tempIds = new List<string>();
					}
					tempIds.Add(id.Id);

				}
				if (tempIds.Any())
				{
					toCall.Add(tempIds);
				}
			}

			foreach (List<string> ids in toCall)
			{
				try
				{
					
					//implement for each dc
					APIResponse AetherRes = await ApiClient.CallAPI("Aether", ids, ts);
					APIResponse CrystalRes = await ApiClient.CallAPI("Crystal", ids, ts);
					APIResponse DynamisRes = await ApiClient.CallAPI("Dynamis", ids, ts);
					APIResponse PrimalRes = await ApiClient.CallAPI("Primal", ids, ts);

					ts.TraceEvent(TraceEventType.Information, 5, "Inserting into DB");

					bool AetherStatus = await InsertToDB("NA", AetherRes, ts);
					bool CrystalStatus = await InsertToDB("NA", CrystalRes, ts);
					bool DynamisStatus = await InsertToDB("NA", DynamisRes, ts);
					bool PrimalStatus = await InsertToDB("NA", PrimalRes, ts);

					//if one or more inserts fails, break out of loop
					if (!AetherStatus)
					{
						ts.TraceEvent(TraceEventType.Warning, 1, "Aether insert/update failed.");
					}
					if (!CrystalStatus)
					{
						ts.TraceEvent(TraceEventType.Warning, 2, "Crystal insert/update failed.");
					}
					if (!DynamisStatus)
					{
						ts.TraceEvent(TraceEventType.Warning, 3, "Dynamis insert/update failed.");
					}
					if (!PrimalStatus)
					{
						ts.TraceEvent(TraceEventType.Warning, 4, "Primal insert/update failed.");
					}
					if (!AetherStatus || !CrystalStatus || !DynamisStatus || !PrimalStatus)
					{
						ts.Flush();
						break;
					}
				}
				catch (Exception e)
				{
					ts.TraceEvent(TraceEventType.Error, 2, e.Message);
					ts.Flush();
				}

			}
			ts.TraceEvent(TraceEventType.Information, 2, "Finished inserting " + toCall.Count + " items into DB!");
			ts.Close();

		}
		
		/// <summary>
		/// Attempts to insert/update Items into the database
		/// </summary>
		/// <param name="dbName"> Name of Database, ie name of region</param>
		/// <param name="apiRes"> API response object, </param>
		/// <returns>boolean, true if no errors, false otherwise</returns>
		static async Task<bool> InsertToDB(string dbName, APIResponse apiRes, TraceSource ts)
		{
			if (apiRes == null)
			{
				ts.TraceEvent(TraceEventType.Warning, 5, "No APIRes passed to db for insert");
				return false;
			}
			try
			{
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
					catch (Exception e)
					{
						ts.TraceEvent(TraceEventType.Error, 3, "Error writing to MongoDB: " + e.Message);
						await session.AbortTransactionAsync();
						return false;
					}

				}

				ts.TraceEvent(TraceEventType.Information, 3, $"Successfully inserted/updated {apiRes.items.Count} items into {dbName} database");
			}
			catch (Exception e)
			{
				ts.TraceEvent(TraceEventType.Error, 4, $"Something went wrong trying to insert the new documents." +
					$" Message: {e.Message}");
				return false;
			}
			return true;
		}


	}
	
}
