using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text.Json;
using System;
using Serilog;
namespace ffxiv
{
    internal class APIClient
    {
		HttpClient client;
		public APIClient()
		{
			client = new HttpClient();
			client.BaseAddress = new Uri("https://universalis.app/api/v2/");
		}
        public async Task<APIResponse> CallAPI(string world, List<string> items, Options options = null)
		{
			//setting default options
			options ??= new Options(50, new string[] { "items.listings.pricePerUnit", "items.averagePrice", "items.unitsSold", "items.listings.worldName", "dcName", "worldName" });
			string uri = $"{world}/{string.Join(",", items)}?listings={options.listings}&fields={string.Join(",", options.fields)}";
			HttpResponseMessage res = await client.GetAsync(uri);
			//should throw an HttpRequestException if the status is not 200-299
			res.EnsureSuccessStatusCode();
			return await ParseRes(res, items);
			
			
		}

		private static async Task<APIResponse> ParseRes(HttpResponseMessage res, List<string> items)
		{
			try
			{
				//convert ContentStream to string
				string apiRes = await res.Content.ReadAsStringAsync();

				List<Item> retItems = new List<Item>();
				//gets index of either worldName or dcName (API call can be made to either a world or DC)
				int dcIndex = apiRes.IndexOf("dcName") > 0 ? apiRes.IndexOf("dcName") : apiRes.IndexOf("worldName");
				string dcName = apiRes.Substring(dcIndex).Split('\"')[2];
				//trim apiRes
				apiRes = apiRes.Substring(0, dcIndex - 2);

				//loops through requested items
				for (int i = 0; i < items.Count; i++)
				{
					
					string item = items[i];
					//determine if item is last in list
					if (i != items.Count - 1)
					{
						int currIndex = apiRes.IndexOf($"\"{item}\"");
						if(currIndex == -1)
						{
							continue;
						}
						//get index of next item, if it is not found, continue looking for the next valid item
						int j = 0;
						int nextIndex = -1;
						for (j = i; nextIndex == -1 && j < items.Count - 1; j++)
						{
							string nextItem = items[j + 1];
							nextIndex = apiRes.IndexOf($"\"{nextItem}\"");
							//makes sure outer loop doesnt try and start with a missing item
							if (nextIndex != -1)
							{
								i = j;
							}
						}

						//if a next item could not be found before the end of the json, just use the rest of the json
						//otherwise take substring of space between the current and next item
						string itemJson = apiRes.Substring(currIndex);
						if (nextIndex != -1)
						{
							itemJson = apiRes.Substring(currIndex, (nextIndex - currIndex));
						}

						//trim itemJson to prepare to insert 
						itemJson = itemJson.Substring(itemJson.IndexOf("{") + 1);
						//add the item ID
						itemJson = $"{{ \"Id\": \"{item}\"," + itemJson;
						//trim comma and quote off
						itemJson = itemJson.Substring(0, itemJson.Length - 1);

						retItems.Add(JsonSerializer.Deserialize<Item>(itemJson));


						//should only occur when the last items are not found, prevents the loop from continuing with no items
						if (nextIndex == -1)
						{
							break;
						}
					}
						
					// is last item
					else
					{
						int currIndex = apiRes.IndexOf($"\"{item}\"");
						//make sure the last item is found
						if (currIndex != -1)
						{
							//since its the last item, take all of string after current item
							string itemJson = apiRes.Substring(currIndex);
							//trim itemJson to prepare to insert 
							itemJson = itemJson.Substring(itemJson.IndexOf("{") + 1);
							//add the item ID
							itemJson = $"{{ \"Id\": \"{item}\"," + itemJson;
							//trim itemJson
							itemJson = itemJson.Substring(0, itemJson.Length - 1);

							retItems.Add(JsonSerializer.Deserialize<Item>(itemJson));
						}
					}

				}
				return new APIResponse(dcName, retItems, items);
			}
			catch(Exception ex) 
			{
				Log.Error("Parsing failed with error {0}, with res: {1}, and items: {2}", ex.Message, await res.Content.ReadAsStringAsync(), items);
				throw;
			}
			
		}

	}
}
