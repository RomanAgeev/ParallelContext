using System.Collections.Generic;
using Guards;

namespace ParallelContext {
        public class LockContext<T> {
        readonly IEnumerator<T> enumerator;
        readonly object _lock = new object();

        public LockContext(IEnumerable<T> enumeration) {
            Guard.NotNull(enumeration, nameof(enumeration));
            
            enumerator = enumeration.GetEnumerator();
        }

        public IEnumerable<T> AsEnumerable() {
            while(true) {
                T result = default(T);
                bool success = false;                
                lock(_lock) {
                    success = enumerator.MoveNext();
                    if(success)
                        result = enumerator.Current;
                }                
                if(success)
                    yield return result;                    
                else 
                    yield break;                    
            }
        }
    }
}