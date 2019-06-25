using System.IO;
using System.IO.Compression;

namespace Kontur.LogPackerGZip
{
    internal class GZipCompressor
    {
        public void Compress(Stream inputStream, Stream outputStream)
        {
            using (var gzipStream = new GZipStream(outputStream, CompressionLevel.Optimal, true))
            {
                //while (true)
                //{
                //    byte[] buffer = new byte[20000];
                //    int count = inputStream.Read(buffer, 0, 20000);
                //    gzipStream.Write(buffer, 0, count);
                //    if (count != 20000) break;
                //}
                inputStream.CopyTo(gzipStream);
            }
        }

        public void Decompress(Stream inputStream, Stream outputStream)
        {
            using (var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress, true))
                gzipStream.CopyTo(outputStream);
        }
    }
}