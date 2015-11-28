#!/bin/bash

for f in ../*.gz; do
    echo ${f}...
    gunzip $f
done

cd ..
touch DATAFEST_TRANSACTIONS.csv

cat DATAFEST_TRANSACTIONS_P20150131.csv >> DATAFEST_TRANSACTIONS.csv
tail -n +2 DATAFEST_TRANSACTIONS_P20150228.csv >> DATAFEST_TRANSACTIONS.csv
tail -n +2 DATAFEST_TRANSACTIONS_P20150331.csv >> DATAFEST_TRANSACTIONS.csv
tail -n +2 DATAFEST_TRANSACTIONS_P20150430.csv >> DATAFEST_TRANSACTIONS.csv
tail -n +2 DATAFEST_TRANSACTIONS_P20150531.csv >> DATAFEST_TRANSACTIONS.csv
tail -n +2 DATAFEST_TRANSACTIONS_P20150630.csv >> DATAFEST_TRANSACTIONS.csv

cd -