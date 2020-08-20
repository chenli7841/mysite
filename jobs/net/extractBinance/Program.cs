using System;
using System.Threading;
using System.Threading.Tasks;

namespace extractBinance
{
    class Program
    {
        private static readonly AutoResetEvent _closingEvent = new AutoResetEvent(false);
        static void Main(string[] args)
        {
            RecordOrderbook job = new RecordOrderbook(args[0]);
            job.execute();
            Console.WriteLine("Press Ctrl + C to cancel!");
            Console.CancelKeyPress += ((s, a) =>
            {
                Console.WriteLine("Bye!");
                _closingEvent.Set();
            });
            _closingEvent.WaitOne();
        }
    }
}
