import pandas as pd
import os


def merge_all_csv(dirname: str):
    '''

    :param dirname: such as 'ouput/1901-01-01'
    :return:
    '''
    filenames = os.listdir(dirname)
    if not filenames:
        return False

    temp_pd = pd.read_csv(os.path.join(dirname, filenames[0]))
    for fname in filenames[1:]:
        p = pd.read_csv(os.path.join(dirname, fname))
        temp_pd = pd.merge(temp_pd, p, on="Date")
    print(temp_pd.head())
    dest_path = os.path.join(dirname, 'merge.csv')
    temp_pd.to_csv(dest_path, index=False)
    print(f'已保存文件到 {dest_path}')


def expand_data(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    '''
    对指定列，从大到小（如日期），将对应的数据细化。expand_data(df, "Date")
    :param df:
    :param col_name:
    :return:
    '''
    df[col_name] = pd.to_datetime(df[col_name])  # 将日期转换为datetime格式
    # 生成从第一天到最后一天每小时的时间点
    time_range = pd.date_range(start=df[col_name].min(), end=df[col_name].max(), freq='h')
    # 设置日期为索引
    df = df.set_index(col_name)
    # 重新索引到每小时的时间点，插值空值
    hourly_df = df.reindex(time_range).interpolate(method='linear')
    # 重置索引
    hourly_df = hourly_df.reset_index().rename(columns={'index': col_name})
    print(hourly_df)
    return hourly_df

