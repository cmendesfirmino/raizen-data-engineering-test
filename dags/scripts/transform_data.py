#!/usr/bin/env python
# coding: utf-8

def transform():
    """
    This function transform the stage data and write as parquet format into clean folder.
    """

    import pandas as pd
    import logging

    path_stage = './data/stage/'
    path_clear = './data/clean/'
    file_name = 'vendas_combustivel'

    #reading two sheets and concatanate both one, because there are same structure
    df = pd.read_excel(f'{path_stage}{file_name}.xls', sheet_name=1)
    df_2 = pd.read_excel(f'{path_stage}{file_name}.xls', sheet_name=2)
    df_concat = pd.concat([df,df_2], ignore_index=True)

    #the data is pivoted by month, here we did unpovoting file
    id_vars = list(df_concat.columns[:4])
    value_vars = list(df_concat.columns[4:-1])
    df = pd.melt(df_concat, id_vars=id_vars, value_vars=value_vars, value_name='volume',var_name='month')

    rename_columns = {
        'COMBUSTÍVEL': 'product_unit',
        'ANO': 'year',
        'REGIÃO': 'region',
        'ESTADO': 'uf'
    }
    df = df.rename(columns=rename_columns)

    #create mapping of months to convert the text to numbers (1 to 12)
    month_map = {}
    for idx, value in enumerate(value_vars):
        month_map[value] =  idx+1

    df['month_number'] = df['month'].map(month_map)

    #create column year month str concating strings, this is used to create a datetime month year reference
    df['year_month_str'] = df['year'].astype(str) + '-' + df['month_number'].astype(str)
    df['year_month'] = pd.to_datetime(df['year_month_str'])
    
    #stracting product name and unit, in this case all unit was the same, but this is usefull if will be change
    df['product'] = df['product_unit'].str[:-5]
    df['unit'] = df['product_unit'].str[-5:].str.extract(r'([\w\d]+)')

    #selecting final columns to output
    final_columns = ['year_month','uf','product', 'unit', 'volume']
    df_output = df[final_columns].copy()

    #get current timestamp and make sure the types
    created_at = pd.Timestamp.today()
    try:
        df_output['year_month'] = df_output['year_month'].dt.date
        df_output['uf'] = df_output['uf'].astype(str)
        df_output['product'] = df_output['product'].astype(str)
        df_output['unit'] = df_output['unit'].astype(str)
        df_output['volume'] = df_output['volume'].astype('float64')
        df_output['created_at'] = created_at
    except BaseException as e:
        logging.error("It's no possible to convert any column." + e)
    
    try:
        df_output.to_parquet(
            path=path_clear + file_name, 
            engine='pyarrow',
            compression='gzip',
            partition_cols=['year_month'])
    except BaseException as e:
        logging.error("It's not possible to write in parquet file." + e)
    return path_clear + file_name


