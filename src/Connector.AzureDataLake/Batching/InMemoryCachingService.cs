using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.Common.Batching
{
    public class InMemoryCachingService<TItem, TConfiguration> : ICachingService<TItem, TConfiguration>
    {
        private readonly List<KeyValuePair<TItem, TConfiguration>> _storage;

        public InMemoryCachingService()
        {
            _storage = new List<KeyValuePair<TItem, TConfiguration>>();
        }

        public void AddItem(TItem item, TConfiguration configuration)
        {
            _storage.Add(new KeyValuePair<TItem, TConfiguration>(item, configuration));
        }

        public void Clear()
        {
            _storage.Clear();
        }

        public int Count()
        {
            return _storage.Count;
        }

        public IQueryable<KeyValuePair<TItem, TConfiguration>> GetItems()
        {
            return _storage.AsQueryable();
        }
    }
}
