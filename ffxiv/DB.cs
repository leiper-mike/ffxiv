using MongoDB.Driver;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace ffxiv
{
	internal class DB
	{
		private IMongoClient DBClient;

        public DB(IMongoClient client)
        {
            DBClient = client;
        }

		/// <summary>
		/// Attempts to insert/update Items into the database
		/// </summary>
		/// <param name="dbName"> Name of Database, ie name of region</param>
		/// <param name="apiRes"> API response object, </param>
		/// <returns>boolean, true if no errors, false otherwise</returns>
		public async Task InsertToDB(List<APIResponse> apiResps)
		{
			if (apiResps == null)
			{
				throw new Exception("No APIResps object passed to DB for insertion");
			}
			List<IMongoCollection<Item>> ItemCollections = new List<IMongoCollection<Item>>();
			foreach (APIResponse apiResp in apiResps)
			{
				ItemCollections.Add(DBClient.GetDatabase(apiResp.regionName).GetCollection<Item>(apiResp.dcName));
			}
			   
			using (var session = await DBClient.StartSessionAsync())
			{
				//setup upsert options
				ReplaceOptions options = new ReplaceOptions();
				options.IsUpsert = true;

				// Begin transaction
				session.StartTransaction();
				foreach(IMongoCollection<Item> collection in ItemCollections)
				{
					APIResponse apiRes = apiResps.Find(a => a.dcName == collection.CollectionNamespace.CollectionName);
					try
					{
						foreach (Item item in apiRes.items)
						{
							collection.ReplaceOne(Builders<Item>.Filter.Where(i => i.Id == item.Id), item, options);
						}
						await session.CommitTransactionAsync();
						Log.Information($"Successfully inserted/updated {apiRes.items.Count} items into {apiRes.dcName} collection");
					}
					catch
					{
						await session.AbortTransactionAsync();
						throw;
					}
				}
				

			}
			//remove or move inside loop
			Log.Information($"Successfully upserted {apiResps.Count} objects into DB");

		}
	}
}
