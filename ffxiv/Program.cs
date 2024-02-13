using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DotNetEnv;
using System.Net.Http;
using System.Web.Script.Serialization;
using MongoDB.Driver;

namespace ffxiv
{
	class Program
	{
		static string baseURL = "https://universalis.app/api/v2/";
		static HttpClient client = new HttpClient();
		static IMongoClient DBClient;
		static async Task Main(string[] args)
		{
			DotNetEnv.Env.TraversePath().Load();
			client.BaseAddress = new Uri(baseURL);
			
			try
			{
				DBClient = new MongoClient(System.Environment.GetEnvironmentVariable("DBCONNECTIONSTRING"));
				Console.WriteLine("Successfully connected to DB");
			}
			catch (Exception e)
			{
				Console.WriteLine("Failed to connect to MongoDB: " + e.Message);
				Console.ReadLine();
				return;
			}
			string dbName = "NA";
			


			var apiRes = await CallAPI("Aether", new List<string> { "5116", "5117", "5118" });
			try
			{
				IMongoCollection<Item> ItemCollection = DBClient.GetDatabase(dbName).GetCollection<Item>(apiRes.dcName);


				List<Item> toUpdate = ItemCollection.Find(Builders<Item>.Filter.Where(i => apiRes.IDs.Contains(i.Id))).ToList();
				IEnumerable<Item> toInsert = apiRes.items.Except(toUpdate, new ItemEqualityComparer());

				using (var session = await DBClient.StartSessionAsync())
				{
					// Begin transaction
					session.StartTransaction();

					try
					{
						ItemCollection.InsertMany(toInsert);
						foreach (Item item in toUpdate)
						{
							ItemCollection.ReplaceOne(Builders<Item>.Filter.Where(i => i.Id == item.Id), item);
						}
						await session.CommitTransactionAsync();
					}
					catch(Exception e)
					{
						Console.WriteLine("Error writing to MongoDB: " + e.Message);
						await session.AbortTransactionAsync();
					}
					
				}
					
				Console.WriteLine($"Successfully inserted/updated {apiRes.items.Count} items into {dbName} database");
			}
			catch (Exception e)
			{
				Console.WriteLine($"Something went wrong trying to insert the new documents." +
					$" Message: {e.Message}");
				Console.WriteLine(e);
				Console.WriteLine();
				Console.ReadLine();
				return;
			}
			Console.ReadLine();
		}

		/// <summary>
		/// Makes a call to the Universalis API requesting the up to date listings, average prices and recent sales of items on the specified world/DC
		/// </summary>
		/// <param name="world">Name of region, Data Center or world https://github.com/xivapi/ffxiv-datamining/blob/master/csv/World.csv </param>
		/// <param name="items">list of item ids https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Item.csv</param>
		/// <param name="options">Options object, specifies how many listings (default 10) and what fields to return</param>
		/// <returns>APIResponse object, with world/DC name and a list of items</returns>
		static async Task<APIResponse> CallAPI(string world, List<string> items, Options options = null)
		{
			//setting default options
			options = options ?? new Options(50, new string[] { "items.listings.pricePerUnit", "items.averagePrice", "items.unitsSold", "items.listings.worldName", "dcName", "worldName" });
			string uri = $"{world}/{string.Join(",",items)}?listings={options.listings}&fields={string.Join(",", options.fields)}";
			HttpResponseMessage res = await client.GetAsync(uri);

			if (res.IsSuccessStatusCode)
			{
				JavaScriptSerializer js = new JavaScriptSerializer();
				//convert ContentStream to string
				string apiRes = await res.Content.ReadAsStringAsync();

				List<Item> retItems = new List<Item>();
				//gets index of either worldName or dcName (API call can be made to either a world or DC)
				int dcIndex = apiRes.IndexOf("dcName") > 0 ? apiRes.IndexOf("dcName") : apiRes.IndexOf("worldName");
				string dcName = apiRes.Substring(dcIndex).Split('\"')[2];
				//trim apiRes
				apiRes = apiRes.Substring(0, dcIndex - 2);

				//loops through requested items
				for(int i = 0; i < items.Count; i++)
				{
					string item = items[i];
					//determine if item is last in list
					if(i != items.Count - 1)
					{
						//get index of next item
						string nextItem = items[i + 1];
						int nextIndex = apiRes.IndexOf(nextItem);
						int currIndex = apiRes.IndexOf(item);
						//take substring of space between the current and next item
						string itemJson = apiRes.Substring(currIndex, (nextIndex - currIndex));
						//trim itemJson to prepare to insert 
						itemJson = itemJson.Substring(itemJson.IndexOf("{")+1);
						//add the item ID
						itemJson = $"{{ \"ID\": \"{item}\"," + itemJson;
						//trim comma and quote off
						itemJson = itemJson.Substring(0,itemJson.Length - 2);

						retItems.Add(js.Deserialize<Item>(itemJson));
					}
					// is last item
					else
					{
						int currIndex = apiRes.IndexOf(item);
						//since its the last item, take all of string after current item
						string itemJson = apiRes.Substring(currIndex);
						//trim itemJson to prepare to insert 
						itemJson = itemJson.Substring(itemJson.IndexOf("{") + 1);
						//add the item ID
						itemJson = $"{{ \"ID\": \"{item}\"," + itemJson;
						//trim itemJson
						itemJson = itemJson.Substring(0, itemJson.Length - 1);

						retItems.Add(js.Deserialize<Item>(itemJson));
					}
						
					
				}
				return new APIResponse(dcName, retItems, items);
			}
			return null;
		}
		
	}
	
}
