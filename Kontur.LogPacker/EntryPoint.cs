namespace Kontur.LogPacker
{
    internal static class EntryPoint
    {
        public static void Main(string[] args)
        {
            if (args.Length == 2)
            {
                new GZipLogStream(args[0], args[1]).Compress();
                return;
            }

            if (args.Length == 3 && args[0] == "-d")
            {
                new GZipLogStream(args[1], args[2]).Decompress();
                return;
            }
            
        }
    }
}
