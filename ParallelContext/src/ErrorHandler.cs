using System;
using System.Collections.Generic;

namespace ParallelContext {
    public class ErrorHandler {
        readonly List<Exception> errors = new List<Exception>();
        readonly object _lock = new object();

        public void Handle(Exception error) {
            lock(_lock) {
                errors.Add(error);
            }
        }

        public void ThrowIfFailed() {
            int count = 0;
            lock(_lock) {
                count = errors.Count;
            }

            if(count > 0)
                throw new AggregateException(errors);
        }
    }
}