using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.Common.Batching
{
    public interface ICachingService<TItem, TConfiguration>
    {
        /// <summary>
        /// Add single item to batch
        /// </summary>
        void AddItem(TItem item, TConfiguration configuration);

        /// <summary>
        /// Get all items from batch
        /// </summary>
        IQueryable<KeyValuePair<TItem, TConfiguration>> GetItems();

        /// <summary>
        /// Return current number of items in batch
        /// </summary>        
        int Count();

        /// <summary>
        /// Clear all items from batch
        /// </summary>
        void Clear();
    }
}
