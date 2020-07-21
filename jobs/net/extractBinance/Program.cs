using System;

namespace extractBinance
{
    class Program
    {
        static void Main(string[] args)
        {
            RecordOrderbook job = new RecordOrderbook(args[0]);
            job.execute();
            Console.ReadLine();
        }
    }
}
