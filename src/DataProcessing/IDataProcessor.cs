using DataProcessing.Data;

namespace DataProcessing.DataProcessing;

public interface IDataProcessor
{
    Task ScheduleDataProcessing(DataWithKey dataWithKey);
}