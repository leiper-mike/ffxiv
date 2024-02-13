using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ffxiv
{
	class Options
	{
		public int listings { get; set; }
		public string[] fields { get; set; }

		public Options(int listings, string[] fields)
		{
			this.listings = listings;
			this.fields = fields;
		}
	}
	class APIResponse
	{
		public string dcName { get; set; }
		public List<Item> items { get; set; }

		public List<string> IDs { get; set; }

		public APIResponse(string dcName, List<Item> items, List<string> IDs)
		{
			this.dcName = dcName;
			this.items = items;
			this.IDs = IDs;
		}
	}
	
	class Item 
	{
		public string Id { get; set; }
		public List<Listing> listings { get; set; }
		public double averagePrice { get; set; }
		public int unitsSold { get; set; }

	}

	class Listing
	{
		public int pricePerUnit { get; set; }
		public string worldName { get; set; }
	}
	class ItemEqualityComparer : IEqualityComparer<Item>
	{
		public bool Equals(Item i1, Item i2)
		{

			if (ReferenceEquals(i1, i2))
				return true;
			if (i2 is null || i1 is null)
				return false;
			return i1.Id == i2.Id;
		}
		public int GetHashCode(Item item) => int.Parse(item.Id) ^ item.unitsSold ^ (int)item.averagePrice;
	}
}
