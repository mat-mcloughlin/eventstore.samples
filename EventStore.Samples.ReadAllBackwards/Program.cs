using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.Samples.ReadAllBackwards
{
    static class Program
    {
        static async Task Main()
        {
            // Connect to Event Store.
            var connection = await GetEventStoreConnection();

            // Generate and save 10 events.
            var events = Enumerable.Range(1, 10).Select(e => ToEventData(new SimpleEvent(e)));
            await connection.AppendToStreamAsync("Sample-SimpleReadWrite", ExpectedVersion.Any, events);

            // Read events from all stream.
            var readEvents = await connection.ReadAllEventsBackwardAsync(Position.End, 1000, false);

            // Output events to console.
            foreach (var e in readEvents.Events)
            {
                if (e.Event.EventType.StartsWith("$")) // Ignore system events
                {
                    continue;
                }

                var deserializedEvent = (SimpleEvent) e.Deserialize();
                Console.WriteLine($"{e.Event.EventStreamId} {deserializedEvent.Index} @ {e.OriginalPosition}");
            }
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