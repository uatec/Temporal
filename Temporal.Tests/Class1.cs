using System;
using Xunit;
using System.Linq;

namespace Temporal.Tests
{
    // see example explanation on xUnit.net website:
    // https://xunit.github.io/docs/getting-started-dotnet-core.html
    public class TopicTests
    {
        [Fact]
        public void TopicLifecycle()
        {
            ITemporalService temporalService = new SimpleTemporalService();
            string topic = Guid.NewGuid().ToString();

            temporalService.Create(topic);
            Assert.True(temporalService.Exists(topic));

            temporalService.Delete(topic);
            Assert.True(!temporalService.Exists(topic));
        }
    }

    public class SubscriptionTests
    {
        [Fact]
        public void SubscriptionLifecycle()
        {
            ITemporalService temporalService = new SimpleTemporalService();
            string topic = Guid.NewGuid().ToString();
            temporalService.Create(topic);

            string subscription = temporalService.Subscribe(topic);

            temporalService.Unsubscribe(topic, subscription);

            temporalService.Delete(topic);
        }

        [Fact]
        public void PubSubLifecycle()
        {
            ITemporalService temporalService = new SimpleTemporalService();
            string topic = Guid.NewGuid().ToString();
            temporalService.Create(topic);

            string subscription = temporalService.Subscribe(topic);

            temporalService.Publish(topic, "some stuff");

            string receivedEvent = temporalService.Consume(topic, subscription).First();
            
            Assert.Equal("some stuff", receivedEvent);

            temporalService.Delete(topic);
        }
    }
}
