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

        [Fact]
        public void MultipleMessagesAreReceived() {
            
            ITemporalService temporalService = new SimpleTemporalService();
            string topic = Guid.NewGuid().ToString();
            temporalService.Create(topic);

            string subscription = temporalService.Subscribe(topic);

            temporalService.Publish(topic, "some stuff");
            temporalService.Publish(topic, "some other stuff");

            string[] receivedEvents = temporalService.Consume(topic, subscription).Take(2).ToArray();
            
            Assert.Equal("some stuff", receivedEvents[0]);
            Assert.Equal("some other stuff", receivedEvents[1]);

            temporalService.Delete(topic);
        }

        [Fact]
        public void PreviouslyPublishedMessagesAreReceivedByNewSubs() {
            
            ITemporalService temporalService = new SimpleTemporalService();
            string topic = Guid.NewGuid().ToString();
            temporalService.Create(topic);

            temporalService.Publish(topic, "some stuff");
            temporalService.Publish(topic, "some other stuff");

            string subscription = temporalService.Subscribe(topic);

            string[] receivedEvents = temporalService.Consume(topic, subscription).Take(2).ToArray();
            
            Assert.Equal("some stuff", receivedEvents[0]);
            Assert.Equal("some other stuff", receivedEvents[1]);

            temporalService.Delete(topic);
        }
    }
}
