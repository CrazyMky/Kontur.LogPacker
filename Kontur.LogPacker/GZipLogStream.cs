using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Kontur.LogPacker
{
    class GZipLogStream
    {
        #region VAR_FOR_COMPRESSOR
        long millisecondsFromStarts;
        long tickFromStart;
        bool isFirstString = true;

        Dictionary<string, byte> libStatusToValue;
        Dictionary<byte, string> libValueToStatus;
        byte lastStatusLibValue = 0;

        #endregion


        #region VAR_STREAM
        readonly int bufferSize;
        string inputFileName;
        string outputFileName;
        static string tmpFileName = "tmp";
        byte[] _buffer;

        #endregion


        public GZipLogStream(string inputFileName, string outputFileName, int bufferSize = 1045576 * 20)
        {
            this.inputFileName = inputFileName;
            this.outputFileName = outputFileName;
            this.bufferSize = bufferSize;
        }

        public void Compressor(string inputFileName, string outputFileName)
        {
            this.inputFileName = inputFileName;
            this.outputFileName = outputFileName;
            Compress();
        }
        public void Compress()
        {
            libStatusToValue = new Dictionary<string, byte>();
            using (FileStream inputFile = File.Open(inputFileName, FileMode.Open))
            {
                MemoryStream mWriterOutput = new MemoryStream();
                _buffer = new byte[bufferSize];
                using (BinaryWriter writerOutput = new BinaryWriter(mWriterOutput))
                {
                    int readByteCount;
                    bool dataOfWriterIsRandom = false;
                    byte charNewLine = 10;

                    void writeData(ref int length)
                    {
                        if (dataOfWriterIsRandom)
                        {
                            WriteBinaryData(writerOutput, _buffer, length);
                            dataOfWriterIsRandom = false;
                        }
                        else CompressAndWriteString(writerOutput, _buffer, length);
                        length = 0;
                    }

                    do
                    {
                        readByteCount = inputFile.Read(_buffer, 0, bufferSize);
                        bool isEndPartOfStream = readByteCount < bufferSize;
                        int pointer = 0;

                        for (int i = 0; i < readByteCount; i++)
                        {
                            _buffer[pointer++] = _buffer[i];
                            if (!dataOfWriterIsRandom && BytesFromRangeUnused(_buffer[i]))
                                dataOfWriterIsRandom = true;

                            if (_buffer[i] == charNewLine)
                                writeData(ref pointer);
                        }
                        if (isEndPartOfStream)
                            writeData(ref pointer);

                    } while (readByteCount == bufferSize);

                    writerOutput.Write((byte)TypeOfWriteData.MarkOfEndStream);
                    mWriterOutput.SetLength(mWriterOutput.Position);
                    mWriterOutput.Position = 0;

                    FileMode fMode = File.Exists(outputFileName) ? FileMode.Truncate : FileMode.Create;
                    using (var fileOutput = File.Open(outputFileName, fMode))
                    using (GZipStream zipFileOutput = new GZipStream(fileOutput, CompressionLevel.Optimal))
                    {
                        FileMode dMode = File.Exists(tmpFileName) ? FileMode.Append : FileMode.Create;
                        using (var tmpFile = File.Open(tmpFileName, FileMode.Append))
                        {
                            mWriterOutput.CopyTo(tmpFile);
                            mWriterOutput.Position = 0;
                        }

                        zipFileOutput.Write(CreateHeader(libStatusToValue));
                        if (File.Exists(tmpFileName))
                        {
                            using (var tmp = File.Open(tmpFileName, FileMode.Open))
                                tmp.CopyTo(zipFileOutput);
                            File.Delete(tmpFileName);
                        }
                    }
                }
            }
        }

        public void Decompress(string inputFileName, string outputFileName)
        {
            this.inputFileName = inputFileName;
            this.outputFileName = outputFileName;
            Decompress();
        }
        public void Decompress()
        {
            libValueToStatus = new Dictionary<byte, string>();
            using (var fileRead = File.Open(inputFileName, FileMode.Open))
            using (GZipStream zipReader = new GZipStream(fileRead, CompressionMode.Decompress, true))
            using (var mReader = new MemoryStream())
            using (var _reader = new BinaryReader(mReader))
            {
                using (MemoryStream mWriter = new MemoryStream())
                using (BinaryWriter writer = new BinaryWriter(mWriter))
                {
                    WrapReader reader = new WrapReader(_reader, zipReader, mReader);
                    bool typeOfWriteDataNoReaded = true;
                    TypeOfWriteData typeData = TypeOfWriteData.String;

                    do
                    {
                        if (typeOfWriteDataNoReaded)
                            typeData = (TypeOfWriteData)reader.ReadByte();
                        else typeOfWriteDataNoReaded = true;

                        if (typeData == TypeOfWriteData.StringLogFormat)
                        {
                            WriteStringLogFormat(reader, writer);
                        }
                        else if (typeData == TypeOfWriteData.String)
                        {
                            WriteString(reader, writer);
                        }
                        else if (typeData == TypeOfWriteData.RandomBytes)
                        {
                            WriteRandomBytesArray(reader, writer, ref typeOfWriteDataNoReaded, ref typeData);
                        }
                        else if (typeData == TypeOfWriteData.Header)
                        {
                            GetHeader(reader);
                        }
                        else break;
                    } while (true);
                    mWriter.Position = 0;
                    FileMode fMode = File.Exists(outputFileName) ? FileMode.Truncate : FileMode.Create;

                    using (var file = File.Open(outputFileName, fMode))
                    {
                        if (File.Exists(tmpFileName))
                        {
                            using (var tmp = File.Open(tmpFileName, FileMode.Open)) tmp.CopyTo(file);
                            File.Delete(tmpFileName);
                        }
                        mWriter.CopyTo(file);
                    }
                }
            }
        }

        #region FUNCTIONS_FOR_COMPRESSOR
        private void CompressAndWriteString(BinaryWriter writer, byte[] array, int length)
        {
            string _string = Encoding.UTF8.GetString(array, 0, length);
            string[] rightSplit = CutDoubleSpace(_string);
            DateTime? dateTime = null;
            long millisecond = 0;

            if (CheckDateString(rightSplit[0]) && rightSplit[4] != null)
            {
                try
                {
                    dateTime = Convert.ToDateTime((rightSplit[0] + " " + rightSplit[1]).Replace(',', '.'));
                    millisecond = Convert.ToInt64(rightSplit[2]);
                }
                catch (Exception)
                {
                    WriteBinaryData(writer, array, length);
                    return;
                }
            }
            else dateTime = null;

            if (dateTime == null || rightSplit[4] == null)
            {   //  Write without compress
                writer.Write((byte)TypeOfWriteData.String);
                writer.Write(array, 0, length);
                writer.Write((byte)TypeOfWriteData.EndString);
            }
            else
            {   //  Write with compress
                writer.Write((byte)TypeOfWriteData.StringLogFormat);
                //  1-st block (Date)
                writer.Write(dateTime.Value.Ticks);

                //  2-nd block (Integer)
                if (isFirstString)
                {
                    writer.Write(millisecond);
                    isFirstString = false;
                    tickFromStart = dateTime.Value.Ticks;
                    millisecondsFromStarts = millisecond;
                }
                else if (GetMs(new DateTime(dateTime.Value.Ticks - tickFromStart)) + millisecondsFromStarts - 1
                    != Convert.ToInt64(rightSplit[2]))
                {
                    writer.Write((byte)0);
                    writer.Write(Convert.ToInt64(rightSplit[2]));
                }
                else writer.Write((byte)1);

                //  3-rd block (Status)
                if (libStatusToValue.ContainsKey(rightSplit[3]))
                {
                    writer.Write((byte)StatusMark.Exist);
                    writer.Write(libStatusToValue[rightSplit[3]]);
                }
                else if (lastStatusLibValue < 254)
                {   //  Add new field in status library
                    libStatusToValue[rightSplit[3]] = lastStatusLibValue++;
                    writer.Write((byte)StatusMark.Exist);
                    writer.Write(libStatusToValue[rightSplit[3]]);
                }
                else
                {
                    writer.Write((byte)StatusMark.NotExist);
                    writer.Write(Encoding.UTF8.GetBytes(rightSplit[3]));
                    writer.Write((byte)StatusMark.EndStatusWord);
                }

                //  4-th block (Message)
                writer.Write(Encoding.UTF8.GetBytes(rightSplit[4]));
                writer.Write((byte)TypeOfWriteData.EndString);
            }

            if (writer.BaseStream.Position > 1045576 * 40)
            {
                FileMode dMode = File.Exists(tmpFileName) ? FileMode.Append : FileMode.Create;
                using (var tmpFile = File.Open(tmpFileName, FileMode.Append))
                {
                    writer.BaseStream.SetLength(writer.BaseStream.Position);
                    writer.BaseStream.Position = 0;
                    writer.BaseStream.CopyTo(tmpFile);
                    writer.BaseStream.Position = 0;
                }
            }
        }

        private void WriteBinaryData(BinaryWriter writer, byte[] array, int length)
        {
            writer.Write((byte)TypeOfWriteData.RandomBytes);
            for (int i = 0; i < length; i++)
            {
                writer.Write(array[i]);
                if (array[i] == 5)
                    writer.Write(array[i]);
            }
            writer.Write((byte)5);
        }

        private bool BytesFromRangeUnused(byte currentChar)
        {
            return currentChar != 10 && currentChar != 13 && currentChar != 9 && !(currentChar >= 32 && currentChar <= 122);
        }

        private string[] CutDoubleSpace(string s)
        {
            string[] resArray = new string[5];
            if (s.Length > 0)
            {
                StringBuilder res = new StringBuilder().Append(s[0]);
                int c = 0;
                for (int i = 1; i < s.Length; i++)
                {
                    if (s[i] == ' ' && s[i - 1] == ' ') continue;
                    else if (s[i] == ' ' && c < 4)
                    {
                        resArray[c++] = res.ToString();
                        res.Clear();
                    }
                    else res.Append(s[i]);
                }
                resArray[c] = res.ToString();
            }
            return resArray;
        }

        private DateTime? CutDate(string s, bool withRewrite = true)
        {
            StringBuilder sb = new StringBuilder();
            if (s[0] == '2') sb = GetDate(s);

            if (sb.Length != 0)
            {
                sb.Replace(',', '.');
                try
                {
                    DateTime dt = Convert.ToDateTime(sb.ToString());
                    return dt;
                }
                catch (Exception)
                {
                    return null;
                }
            }
            return null;
        }

        private StringBuilder GetDate(string s)
        {
            StringBuilder sb = new StringBuilder();
            int spaceCount = 0;
            for (int i = 0; i < s.Length && spaceCount < 2; i++)
            {
                sb.Append(s[i]);
                if (s[i] == ' ') spaceCount++;
            }
            return sb;
        }

        private long GetMs(DateTime date)
        {
            return date.Millisecond + date.Second * 1000 + date.Minute * 60 * 1000 + date.Hour * 60 * 60 * 1000 + (date.Day - 1) * 12 * 60 * 60 * 1000;
        }

        private bool CheckDateString(string date)
        {
            if (date == null) return false;
            int c = 0;
            for (int i = 0; i < date.Length; i++)
            {
                if (Char.IsNumber(date[i])) c++;
            }
            return c == 8;
        }

        private byte[] CreateHeader(Dictionary<string, byte> library)
        {
            if (library.Count == 0) return null;

            List<byte> result = new List<byte>();
            result.Add((byte)TypeOfWriteData.Header);

            foreach (var item in library)
            {
                result.AddRange(Encoding.UTF8.GetBytes(item.Key));
                result.Add((byte)StatusMark.Split);
                result.Add(item.Value);
            }
            result.Add((byte)'\n');
            return result.ToArray();
        }

        #endregion


        #region FUNCTIONS_FOR_DECOMPRESSOR
        private void WriteStringLogFormat(WrapReader reader, BinaryWriter writer)
        {
            byte currentByte;
            long integerOfSecondBlock;
            long dateTicks = reader.ReadInt64();
            string statusWord = string.Empty;
            DateTime date = new DateTime(dateTicks);

            //  Get Integer (2-nd block)    var: integerOfSecondBlock
            if (isFirstString || reader.ReadByte() == 1)
            {
                integerOfSecondBlock = isFirstString ? reader.ReadInt64() : GetMs(new DateTime(dateTicks - tickFromStart)) + millisecondsFromStarts - 1;
            }
            else integerOfSecondBlock = reader.ReadInt64();

            //  Save information about integer of second block
            if (isFirstString)
            {
                isFirstString = false;
                tickFromStart = dateTicks;
                millisecondsFromStarts = integerOfSecondBlock;
            }

            //  Get status (3-rd block)     var: statusWord
            if (reader.ReadByte() == 1)
            {   //  Value exist in library
                byte keyForLib = reader.ReadByte();
                statusWord = libValueToStatus[keyForLib];
            }
            else
            {
                List<byte> statusWordBytes = new List<byte>();
                byte EndStatusWord = (byte)StatusMark.EndStatusWord;

                while ((currentByte = reader.ReadByte()) != EndStatusWord)
                {
                    statusWordBytes.Add(currentByte);
                }
                statusWord = Encoding.UTF8.GetString(statusWordBytes.ToArray());
            }

            byte endStringByte = (byte)TypeOfWriteData.EndString;
            StringBuilder rightPart = new StringBuilder();
            while ((currentByte = reader.ReadByte()) != endStringByte)
            {
                rightPart.Append((char)currentByte);
            }

            long currentPoint = writer.BaseStream.Position;

            writer.BaseStream.Write(Encoding.UTF8.GetBytes(date.Year + "-" +
                AddLeadingZeros(date.Month) + "-" +
                AddLeadingZeros(date.Day) + " " +
                AddLeadingZeros(date.Hour) + ":" +
                AddLeadingZeros(date.Minute) + ":" +
                AddLeadingZeros(date.Second) + "," +
                AddLeadingZeros(date.Millisecond, 3)));
            writer.BaseStream.Write(Encoding.UTF8.GetBytes(" " + integerOfSecondBlock));
            writer.BaseStream.Write(Encoding.UTF8.GetBytes(new string(' ', Math.Max(1, 31 - ((int)writer.BaseStream.Position - (int)currentPoint)))));
            writer.BaseStream.Write(Encoding.UTF8.GetBytes(statusWord));
            writer.BaseStream.Write(Encoding.UTF8.GetBytes(new string(' ', Math.Max(1, 37 - ((int)writer.BaseStream.Position - (int)currentPoint)))));
            writer.BaseStream.Write(Encoding.UTF8.GetBytes(rightPart.ToString()));
        }

        private void WriteRandomBytesArray(WrapReader reader, BinaryWriter writer, ref bool typeAlreadyRead, ref TypeOfWriteData typeWriteData)
        {
            byte endString = (byte)TypeOfWriteData.EndString;
            List<byte> bytesToWrite = new List<byte>();
            do
            {
                byte currentByte = reader.ReadByte();
                if (currentByte == endString)
                {
                    currentByte = reader.ReadByte();
                    if (currentByte != endString)
                    {
                        //  Save information about type next string
                        typeAlreadyRead = false;
                        typeWriteData = (TypeOfWriteData)currentByte;
                        break;
                    }
                }
                bytesToWrite.Add(currentByte);
            } while (true);
            writer.BaseStream.Write(bytesToWrite.ToArray());
        }

        private void WriteString(WrapReader reader, BinaryWriter writer)
        {
            byte endString = (byte)TypeOfWriteData.EndString;
            byte currentByte;
            while ((currentByte = reader.ReadByte()) != endString)
            {
                writer.Write(currentByte);
            }
        }

        private string AddLeadingZeros(int value, short length = 2)
        {
            if (length - value.ToString().Length != 0)
                return new string('0', length - value.ToString().Length) + value;
            else return value.ToString();
        }

        private void GetHeader(WrapReader reader)
        {
            Dictionary<byte, string> statusLib = new Dictionary<byte, string>();
            List<byte> keyFieldList = new List<byte>();
            string key = string.Empty;
            byte currentByte = 0;
            byte value;
            byte split = (byte)StatusMark.Split;

            while ((currentByte = reader.ReadByte()) != (byte)'\n')
            {
                if (currentByte == split)
                {
                    key = Encoding.UTF8.GetString(keyFieldList.ToArray());
                    value = reader.ReadByte();
                    statusLib[value] = key;
                    keyFieldList = new List<byte>();
                }
                else keyFieldList.Add(currentByte);
            }
            libValueToStatus = statusLib;
        }

        #endregion

    }
}
