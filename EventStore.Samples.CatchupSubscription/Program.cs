using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.Samples.CatchupSubscription
{
    class Program
    {
        static async Task Main()
        {
            // Connect to Event Store.
            var connection = await GetEventStoreConnection();

            // Generate and save 10 events.
            var eventsBeforeSub = Enumerable.Range(1, 10).Select(e => ToEventData(new SimpleEvent(e)));
            await connection.AppendToStreamAsync("Sample-SimpleSubscription", ExpectedVersion.Any, eventsBeforeSub);
            
            // Subscribe to all stream.
            connection.SubscribeToAllFrom(Position.Start, CatchUpSubscriptionSettings.Default, (s, e) =>
            {
                if (e.Event.EventType.StartsWith("$")) // Ignore system events
                {
                    Console.WriteLine($"{e.Event.EventStreamId} @ {e.OriginalPosition}");
                    return Task.CompletedTask;
                }

                var deserializedEvent = (SimpleEvent) e.Deserialize();
                Console.WriteLine($"{e.Event.EventStreamId} {deserializedEvent.Index} @ {e.OriginalPosition}");

                return Task.CompletedTask;
            }, s =>
            {
                Console.WriteLine("Live processing started.");
            } );

            // Generate 10 events
            var events = Enumerable.Range(11, 20).Select(e => ToEventData(new SimpleEvent(e))).ToList();

            // Save each event in time.
            foreach (var e in events)
            {
                Thread.Sleep(500);
                await connection.AppendToStreamAsync("Sample-SimpleSubscription", ExpectedVersion.Any, e);
            }

            Console.ReadLine();
        }

        private static async Task<IEventStoreConnection> GetEventStoreConnection()
        {
            const string connectionString = "ConnectTo=tcp://admin:changeit@localhost:1113";

            var connectionSettings = ConnectionSettings.Create()
                .KeepReconnecting()
                .KeepRetrying();

            var eventStoreConnection =
                EventStoreConnection.Create(connectionString, connectionSettings);

            await eventStoreConnection.ConnectAsync();
            return eventStoreConnection;
        }

        private static EventData ToEventData(object e)
        {
            return new EventData(
                Guid.NewGuid(),
                e.GetType().Name,
                true,
                e.Serialize(),
                null);
        }
    }

    public class SimpleEvent
    {
        public int Index { get; }

        public SimpleEvent(int index)
        {
            Index = index;
        }
    }

    public static class EventStoreHelpers
    {
        public static object Deserialize(this ResolvedEvent resolvedEvent) =>
            JsonConvert.DeserializeObject(Encoding.UTF8.GetString(resolvedEvent.Event.Data),
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Objects});

        public static byte[] Serialize(this object e) =>
            Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(e,
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Objects}));
    }
}