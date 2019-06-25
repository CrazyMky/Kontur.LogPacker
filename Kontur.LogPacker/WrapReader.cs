using System;
using System.IO;
using System.IO.Compression;

namespace Kontur.LogPacker
{
    class WrapReader
    {
        private readonly int bufferSize;
        BinaryReader reader;
        GZipStream zipStream;
        MemoryStream mStream;

        public WrapReader(BinaryReader reader, GZipStream zipStream, MemoryStream mStream, int bufferSize = 1045576 * 20)
        {
            this.reader = reader;
            this.zipStream = zipStream;
            this.bufferSize = bufferSize;
            this.mStream = mStream;
        }

        private void AddData(int saveCountElemeent = 0)
        {
            if (saveCountElemeent != 0)
            {
                byte[] buffer = mStream.GetBuffer();

                if (saveCountElemeent > buffer.Length) throw new ArgumentException("Количетсво элементов, которые необходимо сохранить, превышает длину буфера");

                for (int i = 0; i < saveCountElemeent; i++)
                    buffer[i] = buffer[buffer.Length - saveCountElemeent + i];
            }

            mStream.SetLength(bufferSize);
            int count = zipStream.Read(mStream.GetBuffer(), saveCountElemeent, bufferSize - saveCountElemeent);
            mStream.SetLength(count + saveCountElemeent);
            mStream.Position = 0;
        }

        public byte ReadByte()
        {
            int res = mStream.ReadByte();
            if (res < 0)
            {
                AddData();
                res = mStream.ReadByte();
            }
            return (byte)res;
        }

        public long ReadInt64()
        {
            if (reader.BaseStream.Length - reader.BaseStream.Position < 8)
                AddData((int)(reader.BaseStream.Length - reader.BaseStream.Position));
            return reader.ReadInt64();
        }

        public char ReadChar()
        {
            if (reader.BaseStream.Length - reader.BaseStream.Position < 4)
                AddData((int)(reader.BaseStream.Length - reader.BaseStream.Position));
            return reader.ReadChar();
        }
    }
}
