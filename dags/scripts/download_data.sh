#!/bin/bash
#download_data.sh

curl -L -o /opt/airflow/data/raw/vendas_combustivel.xls \
    https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls