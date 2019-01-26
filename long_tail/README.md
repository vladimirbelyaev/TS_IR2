Для корректной работы:

- Положить файлы из kaggle в TestDir/behrank_data

- До работы в hadoop запустить TestDir/behrank_data/fix_urls.py, get_all_pairs.py и get_sets.py
- Посчитать IDF, ICF, сгенерировать запросы с значениями IDF/ICF через gen_queries_df.py, gen_queries_cf.py, затем посчитать BM25, BM25ICF и поведенческие фичи в mapreduce, положить их в TestDir/BehrankFeatures
- Запустить jupyter-блокнот Final_Prediction.ipynb