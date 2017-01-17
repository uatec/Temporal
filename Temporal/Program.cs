using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Temporal
{
    public class Program
    {
        public static void Main(string[] args)
        {
        }
    }

    public class Topic
    {
        public string Name { get; set; }
    }

    public class SimpleTemporalService : ITemporalService
    {
        private readonly string _dataDirectory;

        public SimpleTemporalService(string dataDirectory = "data")
        {
            _dataDirectory = dataDirectory;
        }

        public IEnumerable<string> Consume(string topicName, string subscription)
        {
            while ( true ) {
                string currentPositionStr = File.ReadAllText(Path.Combine(_dataDirectory, topicName, "subscriptions", subscription));
                Int64 currentPosition = Int64.Parse(currentPositionStr);
                using ( var fileStream = File.OpenRead(Path.Combine(_dataDirectory, topicName, "tlog.dat")))
                using ( TextReader tr = new StreamReader(fileStream))
                {
                    fileStream.Seek(currentPosition, SeekOrigin.Begin);
                    string nextLine = tr.ReadLine();
                    File.WriteAllText(Path.Combine(_dataDirectory, topicName, "subscriptions", subscription), (currentPosition + nextLine.Length + Environment.NewLine.Length).ToString());
                    yield return nextLine;
                }
            }
        }

        public void Create(string name)
        {
            if ( !this.Exists(name) ) {
                var di = Directory.CreateDirectory(Path.Combine(_dataDirectory, name));
                using ( File.Create(Path.Combine(di.FullName, "tlog.dat"))) {}
            }
        }

        public void Delete(string name)
        {
            if ( this.Exists(name)  ) {
                Directory.Delete(Path.Combine(_dataDirectory, name), true);
            }
        }

        public bool Exists(string name)
        {
            return Directory.Exists(Path.Combine(_dataDirectory, name));
        }

        public void Publish(string topicName, string @event)
        {
            File.AppendAllLines(Path.Combine(Path.Combine(_dataDirectory, topicName), "tlog.dat"), new string[]{@event});
        }

        public string Subscribe(string topicName)
        {
            if ( !this.Exists(topicName) ) throw new Exception("Invalid topic name");
            
            string subscriptionId = Guid.NewGuid().ToString();

            if ( !Directory.Exists(Path.Combine(_dataDirectory, topicName, "subscriptions"))) 
                Directory.CreateDirectory(Path.Combine(_dataDirectory, topicName, "subscriptions"));

            File.WriteAllText(Path.Combine(_dataDirectory, topicName, "subscriptions", subscriptionId), "0");

            return subscriptionId;
        }

        public void Unsubscribe(string topicName, string subscriptionId)
        {
            if ( !this.Exists(topicName) ) throw new Exception("Invalid topic name");

            File.Delete(Path.Combine(_dataDirectory, topicName, "subscriptions", subscriptionId));
        }
    }
    
    public interface ITemporalService
    {
        bool Exists(string name);
        void Create(string name);
        void Delete(string name);
        void Publish(string topicName, string @event);
        string Subscribe(string topicName);
        void Unsubscribe(string topicName, string subscriptionId);
        IEnumerable<string> Consume(string topicName, string subscription);
    }
}
