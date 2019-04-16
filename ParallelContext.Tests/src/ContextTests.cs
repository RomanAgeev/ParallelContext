using Xunit;
using FluentAssertions;
using FakeItEasy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CancellationToken = ParallelCore.CancellationToken;

namespace ParallelContext.Tests {
    
    public class ContextTests {
        const int timeout = 5000;

        static IEnumerable<int> Sequence(int count, int interval = 0) {
            return Enumerable.Range(1, count)
                .Select(x => { 
                    Thread.Sleep(interval);
                    return x;
                });
        }

        [Theory]
        [InlineData(2)]
        [InlineData(5)]
        [InlineData(10)]
        public void LockContext_Test(int jobCount) {
            const string enter = "Enter";
            const string leave = "Leave";

            int sequenceLength = jobCount * 10;

            var trace = new List<string>();

            Task.Run(() => {
                Sequence(sequenceLength)
                    .Select(x => {    
                        trace.Add(enter);
                        Thread.Sleep(10);
                        trace.Add(leave);
                        return x;
                    })
                    .AsParallel(jobCount)
                    .AsEnumerable()
                    .ToList();
            })
            .Wait(timeout)
            .Should().BeTrue("Timeout");

            trace.Should().HaveCount(sequenceLength * 2);
            for(int i = 0; i < trace.Count; i += 2) {
                trace[i].Should().Be(enter);
                trace[i + 1].Should().Be(leave);
            }
        }

        [Theory]
        [InlineData(2)]
        [InlineData(5)]
        [InlineData(10)]
        public void NormalProcessing_Test(int jobCount) {
            int sequenceLength = jobCount * 10;

            List<int> temp = new List<int>(sequenceLength);
            List<string> results = null;

            Func<int, int> transform1 = x => x * 10;
            Func<int, string> transform2 = x => $"Value : {x}";

            Task.Run(() => {
                results = Sequence(sequenceLength, 5)
                    .AsParallel(jobCount)
                    .Map(transform1)
                    .Do(x => {                        
                        lock(temp) {
                            temp.Add(x);
                        }
                    })
                    .Map(transform2)
                    .AsEnumerable()
                    .ToList();
            })
            .Wait(timeout)
            .Should().BeTrue("Timeout");

            temp.Should().BeEquivalentTo(Enumerable
                .Range(1, sequenceLength)
                .Select(transform1));
            
            results.Should().BeEquivalentTo(Enumerable
                .Range(1, sequenceLength)
                .Select(transform1)
                .Select(transform2));
        }
        
        [Theory]
        [InlineData(2)]
        [InlineData(5)]
        [InlineData(10)]
        public void Cancellation_Test(int jobCount) {
            int sequenceLength = jobCount * 10;
            int cancelTreshold = jobCount * 2;
            int resultTreshold = jobCount * 3;

            var cancellationEvent = new AutoResetEvent(false);
            var cancellationToken = new CancellationToken();

            List<int> results = new List<int>();

            Task task = Task.Run(() => {
                 IEnumerable<int> values = Sequence(sequenceLength, 5)
                    .AsParallel(jobCount)
                    .AsEnumerable(cancellationToken);
                
                foreach(int value in values) {                    
                    results.Add(value);
                    if(results.Count == cancelTreshold)
                        cancellationEvent.Set();
                }
            });
            
            cancellationEvent.WaitOne();
            cancellationToken.Cancel();

            task.Wait(timeout)
                .Should().BeTrue("Timeout");

            results.Should().HaveCountLessOrEqualTo(resultTreshold);
        }

        [Fact]    
        public void JobLogger_Test() {
            var fakeLogger = A.Fake<IJobLogger>();

            Task.Run(() => {
                Sequence(10, 5)
                    .AsParallel(5)
                    .AsEnumerable(null, fakeLogger)
                    .ToList();
            })
            .Wait(timeout)
            .Should().BeTrue("Timeout");

            A.CallTo(() => fakeLogger.LogResultsCount(A<int>._))
                .MustHaveHappened();
        }

        [Fact]
        public void ErrorHandling_Test() {
            List<int> results = null;
            
            Action act = () => {            
                Task.Run(() => {
                    results = Sequence(50, 5)
                        .AsParallel(5)
                        .Do(_ => {
                            throw new InvalidOperationException();
                        })
                        .AsEnumerable()
                        .ToList();
                })
                .Wait(timeout)
                .Should().BeTrue("Timeout");
            };

            act.Should().Throw<InvalidOperationException>();
        }
    }
}
