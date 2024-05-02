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
using System.Threading;
using Polly.Retry;
using Serilog;
using System.Net.Http;
using Quartz;
using Quartz.Impl;
using Quartz.Logging;
using System.Collections.Specialized;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver.Core.Configuration;

namespace ffxiv
{
	class Program
	{
		public static IMongoClient DBClient;
		static async Task Main(string[] args)
		{
			Env.TraversePath().Load();

			Log.Logger = new LoggerConfiguration().WriteTo.Console().WriteTo.File(Path.GetFullPath("./logs/log.txt", Environment.CurrentDirectory)).CreateLogger();
			
			Log.Information("Starting program.");

			try
			{
				DBClient = new MongoClient(Environment.GetEnvironmentVariable("DBCONNECTIONSTRING"));
				Log.Information("Successfully connected to DB");
			}
			catch (Exception e)
			{
				Log.Fatal("Failed to connect to MongoDB: " + e.Message);
				throw;
			}
			var builder = Host.CreateDefaultBuilder()
			.ConfigureServices((cxt, services) =>
			{
				services.AddQuartz();
				services.AddQuartzHostedService(opt =>
				{
					opt.WaitForJobsToComplete = true;
				});
			}).Build();

			var schedulerFactory = builder.Services.GetRequiredService<ISchedulerFactory>();
			var scheduler = await schedulerFactory.GetScheduler();
			
			IJobDetail updateJob = JobBuilder.Create<UpdateJob>()
				.WithIdentity("update", "group1")
				.DisallowConcurrentExecution()
				.Build();
			//died after 50 minutes, sockets/security exceptions, need to swap to dependency injection for APIclient, make connection pool for DB, do db first since it died during upsert
			ITrigger hourlyTrigger = TriggerBuilder.Create()
				.WithIdentity("hourly", "group1")
				.StartNow()
				.WithSimpleSchedule(x => x
					.WithIntervalInMinutes(45)
					.RepeatForever())
				.Build();

			await scheduler.ScheduleJob(updateJob, hourlyTrigger);
			await builder.RunAsync();
			await Log.CloseAndFlushAsync();
		}
	}

	internal class UpdateJob : IJob
	{
		public async Task Execute(IJobExecutionContext context)
		{
			Log.Information($"Update job: {context}");
			CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
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

			DB database = new(Program.DBClient);

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
			// for logging
			int count = 0;
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
					List<APIResponse> respsNA = new List<APIResponse> { AetherRes, CrystalRes, DynamisRes, PrimalRes };
					foreach (APIResponse res in respsNA)
					{
						res.regionName = "NA";
					}
					Log.Information("Finished calling API, Upserting into DB");

					await database.InsertToDB(respsNA);

					count++;
					Log.Information("Finished upserting batch {0}-{1}, {2} of {3} batches", ids[0], ids[^1], count, toCall.Count);
				}
				catch (Exception e)
				{
					Log.Error(e.Message);
				}

			}
			Log.Information("Finished inserting " + toCall.Count + " batches into DB!");
		}
	}
}
