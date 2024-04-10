using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using Microsoft.Extensions.Logging;

namespace ffxiv
{
    internal sealed class APIClient
    {
		HttpClient client;
		public APIClient(HttpClient client)
		{
			this.client = client;
		}
        public async Task<APIResponse> CallAPI(string world, List<string> items, TraceSource ts, Options options = null)
		{
			//setting default options
			options = options ?? new Options(50, new string[] { "items.listings.pricePerUnit", "items.averagePrice", "items.unitsSold", "items.listings.worldName", "dcName", "worldName" });
			string uri = $"{world}/{string.Join(",", items)}?listings={options.listings}&fields={string.Join(",", options.fields)}";
			HttpResponseMessage res = await client.GetAsync(uri);

			if (res.IsSuccessStatusCode)
			{
				return await ParseRes(res, items);
			}
			else
			{
				ts.TraceEvent(TraceEventType.Error, 1, "API call failed with code: " + res.ReasonPhrase);
				return null;
			}
			
		}

		private static async Task<APIResponse> ParseRes(HttpResponseMessage res, List<string> items)
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
			for (int i = 0; i < items.Count; i++)
			{
				string item = items[i];
				//determine if item is last in list
				if (i != items.Count - 1)
				{
					//get index of next item, if it is not found, continue looking for the next valid item
					int j = 0;
					int nextIndex = -1;
					for (j = i; nextIndex == -1 && j < 100; j++)
					{
						string nextItem = items[j + 1];
						nextIndex = apiRes.IndexOf($"\"{nextItem}\"");
						//makes sure outer loop doesnt try and start with a missing item
						if (nextIndex != -1)
						{
							i = j;
						}
					}

					int currIndex = apiRes.IndexOf($"\"{item}\"");

					//take substring of space between the current and next item
					string itemJson = apiRes.Substring(currIndex, (nextIndex - currIndex));

					//if a next item could not be found before the end of the json, just use the rest of the json
					if (nextIndex == -1)
					{
						itemJson = apiRes.Substring(currIndex);
					}

					//trim itemJson to prepare to insert 
					itemJson = itemJson.Substring(itemJson.IndexOf("{") + 1);
					//add the item ID
					itemJson = $"{{ \"ID\": \"{item}\"," + itemJson;
					//trim comma and quote off
					itemJson = itemJson.Substring(0, itemJson.Length - 1);

					retItems.Add(js.Deserialize<Item>(itemJson));
				}
				// is last item
				else
				{
					int currIndex = apiRes.IndexOf($"\"{item}\"");
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

	}
}
