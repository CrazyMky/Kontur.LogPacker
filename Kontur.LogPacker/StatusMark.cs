namespace Kontur.LogPacker
{
    enum StatusMark
    {
        NotExist,
        Exist,
        EndStatusWord,
        //  Разделитель значений статуса в заголовке файла после сжатия. Диапазон значений для статуса 0-254. Разделитель 255.
        Split = 255
    }
}
