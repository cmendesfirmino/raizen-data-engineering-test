def write_to_postgres(path):
    """
    Function that persists data to Postgresql. This use pandas dataframe to sql function.
    path: directory where parquet files is in.
    """

    import pandas as pd
    from urllib.parse import quote_plus
    import logging
    from sqlalchemy import create_engine
    from airflow.models import Variable

    db_user = Variable.get('POSTGRES_USER')
    db_password = quote_plus(Variable.get('POSTGRES_PASSWORD'))
    db_server = Variable.get('POSTGRES_SERVER')
    db_port = 5432
    db_name = Variable.get('POSTGRES_DW')
    
    conn = create_engine(f'postgresql://{db_user}:{db_password}@{db_server}:{db_port}/{db_name}')
    df = pd.read_parquet(path)

    try:
        df.to_sql('vendas_combustivel', conn, index=False, if_exists='replace', method='multi', chunksize=1000)
    except BaseException as e:
        logging.error(e)
        raise ValueError('Failed to persist data to postgres, please check the error.' )
