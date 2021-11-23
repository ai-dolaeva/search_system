import os
from preprocess import get_query_terms


class Query:
    """
    класс содержит методы реализации булева поиска
    """

    @staticmethod
    def and_query(post_list_1, post_list_2):
        """
        пересечение
        def and_query(self, post_list_1, post_list_2):
        """

        # если пересекать не с чем, то возвращаем пустой список
        if min(len(post_list_1), len(post_list_2)) == 0:
            return []
        result_list = []
        len_1 = len(post_list_1)
        len_2 = len(post_list_2)
        i_1 = 0
        i_2 = 0
        # пока не достигнут конец меньшего из списков
        while i_1 < len_1 and i_2 < len_2:
            d_1 = post_list_1[i_1]
            d_2 = post_list_2[i_2]
            if d_1 == d_2:
                result_list.append(d_1)
                i_1 += 1
                i_2 += 1
            else:
                if d_1 < d_2:
                    i_1 += 1
                else:
                    i_2 += 1

        return result_list

    @staticmethod
    def or_query(post_list_1, post_list_2):
        """
        объединение
         def or_query(self, post_list_1, post_list_2):
        """
        result_list = []
        len_1 = len(post_list_1)
        len_2 = len(post_list_2)
        i_1 = 0
        i_2 = 0
        # пока не достигнут конец меньшего из списков
        while i_1 < len_1 and i_2 < len_2:
            d_1 = post_list_1[i_1]
            d_2 = post_list_2[i_2]
            if d_1 > d_2:
                result_list.append(d_2)
                i_2 += 1
            elif d_1 < d_2:
                result_list.append(d_1)
                i_1 += 1
            else:
                result_list.append(d_1)
                i_1 += 1
                i_2 += 1
        # добаляем оставшиеся индексы
        result_list += post_list_1[i_1:]
        result_list += post_list_2[i_2:]

        return result_list

    @staticmethod
    def not_query(query, index):
        """
        отрицание
        def not_query(self, query, index):
        """

        dir_path = "docs_data/"
        # если терма не нашлась в списке индексов, то возвращаем все индексы
        if query not in index.keys():
            return index
        # определяем кол-во индексов
        index_docs = range(len(os.listdir(dir_path)))
        # список индексов для исключения
        post_list = index[query]
        query_list = []
        i_1 = 0
        i_2 = 0
        len_list = len(post_list)
        # пока не достигнут конец списка с вхождением термов
        while i_1 < len_list:
            doc_1 = post_list[i_1]
            doc_2 = index_docs[i_2]
            if doc_1 > doc_2:
                query_list.append(doc_2)
                i_2 += 1
            else:
                i_1 += 1
                i_2 += 1
        # добавляем все осавшиеся индексы в корпусе
        query_list += index_docs[i_2:]
        return query_list

    def process_query(self, query_list, index):
        """
        нормализация запроса и реализация булевого поиска
        """

        # лемматизация запроса
        temp_list = get_query_terms(query_list)
        result_list = []
        i = 0
        # пока не достигнут конец списка темамов запроса
        while i < len(temp_list):
            if temp_list[i].lower() == 'not':
                i += 1
                result_list += self.not_query(temp_list[i], index)
            elif temp_list[i].lower() == 'and':
                i += 1
                # если and not
                if temp_list[i].lower() == 'not':
                    i += 1
                    result = self.not_query(temp_list[i], index)
                else:
                    result = index[temp_list[i]]
                result_list = self.and_query(result_list, result)
            elif temp_list[i].lower() == 'or':
                i += 1
                # если or not
                if temp_list[i].lower() == 'not':
                    i += 1
                    result = self.not_query(temp_list[i], index)
                else:
                    result = index[temp_list[i]]
                result_list = self.or_query(result_list, result)

            elif temp_list[i] in index.keys():
                result_list = self.or_query(result_list, index[temp_list[i]])
            i += 1
        return result_list
