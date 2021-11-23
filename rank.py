import pickle
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
from preprocess import get_query_terms

# путь для сохранения объединенного индекса
INDEX_PATH = "index/"


def rank(query_list, result_list):
    """
    ранжирование результатов запроса и возвращение топ 10 документов
    def rank(self, query_list, result_list):
    """

    token_list = []
    query_list = get_query_terms(query_list)
    for token in query_list:
        if token.lower() not in ['and', 'or', 'not']:
            token_list.append(token)
    query_list = token_list

    with open(f'{INDEX_PATH}tf.pickle', 'rb') as tf_inf_file:
        tf_idf = pickle.load(tf_inf_file)
    top_doc = pd.DataFrame(columns=['doc_id', 'tf_idf'])
    # для каждого документа в списке номеров документов, соответствующих
    # запросу
    for doc_id in tqdm(result_list, ascii=True,
                       desc='get top 100 documents'):
        sum_tf_idf = 0
        num = 0
        # для каждого терма в запросе
        for term in query_list:
            if term in tf_idf[doc_id]:
                # находим все tf-idf для каждого терма в каждом
                # документе и вычисляем среднее значение
                tf: float = tf_idf[doc_id][term]
                sum_tf_idf += tf
                num += 1
        # формируем dataframe со списком документов и соответствующих
        # средних tf-idf запроса
        top_doc = top_doc.append(
            {'doc_id': doc_id, 'tf_idf': (sum_tf_idf / num)},
            ignore_index=True)
    # сортируем по tf_idf и отбираем первые 100
    top_doc = top_doc.sort_values("tf_idf", ascending=False)
    top_100 = [int(i) for i in list(top_doc['doc_id'])[:100]]
    # 3001 индекс для запроса
    list_ind = [3001] + top_100
    # формируем таблицу как вектора документов равные tf_idf и вектор
    # запроса - 1 или 0
    rank_table = pd.DataFrame(columns=query_list, index=list_ind)
    for doc_id in top_100:
        # для каждого терма в запросе находим tf-idf и записываем в
        # соотвествующую ячейку
        for term in query_list:
            rank_table[term] = 0.0
            rank_table.loc[3001, term] = 1.0
            if term in tf_idf[doc_id]:
                rank_table.loc[doc_id, term] = float(tf_idf[doc_id][term])
    # для документа в списке 100 находим 100 термов с высоким tf-idf и
    # добавляем в dataframe
    for doc_id in tqdm(top_100, ascii=True, desc='make tf-idf table'):
        top_words = list(tf_idf[doc_id].items())[:100]
        for term in top_words:
            if term[0] not in list(rank_table.columns):
                rank_table[term[0]] = 0.0
            rank_table.loc[doc_id, term[0]] = float(term[1])
    table = rank_table.to_numpy()
    # находим косинусную близость векторов
    cos_similarity = cosine_similarity(table[:1], table[1:])
    cos_list = [0] + list(cos_similarity[0])
    rank_table['rank'] = cos_list
    # сортируем по значению косинусной близости векторов
    rank_table = rank_table.sort_values("rank", ascending=False)
    # возвращаем топ 10
    result = list(rank_table.index)[:10]
    if 3001 in result:
        result.remove(3001)
    return result
