import os
from collections import OrderedDict
import pickle
import re
import ast
from tqdm import tqdm
from preprocess import get_terms

# устанавливаем лимит 1MB для предотвращения переполнения памяти
MEMORY_SIZE = 1024 * 1024
# путь для прочтения корпуса
DIR_PATH = "docs_data/"
# путь для хранения промежуточных файлов блочного индексирования
ARCHIVE_PATH = "archive/"
# путь для сохранения объединенного индекса
INDEX_PATH = "index/"


class GenerateIndex:
    """
    класс содержит методы реализации SPIMI индексирования
    и получения значений TF-IDF
    """

    def __init__(self):
        """
        инициализация класса, создания директорий
        def __init__(self):
        """

        if not os.path.exists(ARCHIVE_PATH):
            os.mkdir(ARCHIVE_PATH)
        if not os.path.exists(INDEX_PATH):
            os.mkdir(INDEX_PATH)

    def blocks_splitting(self):
        """
        создание промежуточных файлов блочного индексирования
        def blocks_splitting(self):
        """

        doc_list = os.listdir(DIR_PATH)
        block_name = 0
        doc_block = []
        block_size = 0
        doc_word_freq = {}
        # прочитываем все файлы в корпуе
        for doc_l in tqdm(doc_list, ascii=True, desc='indexing'):
            doc_path = f'{DIR_PATH}{doc_l}'
            term_postings_list = OrderedDict()
            # собираем список имен документов, не превышающих лимита
            doc_block.append(doc_l)
            block_size += os.path.getsize(doc_path)
            if block_size > MEMORY_SIZE:
                # для каждого документа из собранного блока запускаем
                # токенизацию и лемматизацию термов
                for doc_b in doc_block:
                    doc_path = f'{DIR_PATH}{doc_b}'
                    terms = get_terms(doc_path)
                    # id из названия
                    document_id = int(re.search(r'\d+', doc_b)[0])
                    sum_doc_words = 0
                    tf_dict = {}
                    # проверяем существует ли уже терм в словаре
                    for term in terms:
                        if term not in term_postings_list:
                            term_postings_list[term] = [document_id]
                            sum_doc_words += 1
                        # если этого индекса нет в posting list
                        elif document_id not in term_postings_list[term]:
                            term_postings_list[term].append(document_id)
                        # если терм в документе встречался, увеличиваем частоту
                        if term not in tf_dict:
                            tf_dict[term] = 1
                        else:
                            tf_dict[term] += 1
                            sum_doc_words += 1
                    # вычисляем tf, как частота слова в документе
                    # нормализованное общим числом слов в данном документе
                    for i in tf_dict:
                        tf_dict[i] /= sum_doc_words
                    # формируем словарь частот
                    doc_word_freq[document_id] = tf_dict
                # сортируем термы
                term_postings_list = OrderedDict(
                    sorted(term_postings_list.items()))
                # сохраняем промежуточный блочный индекс
                self.write_to_file(term_postings_list,
                                   f'{ARCHIVE_PATH}{block_name}.txt')
                block_name += 1
                block_size = 0
                doc_block = []
        # сохраняем общий словарь частот терм
        with open(f'{INDEX_PATH}tf.pickle', 'wb') as file:
            pickle.dump(doc_word_freq, file)
        print('всё!')

    @staticmethod
    def write_to_file(post_list, path):
        """
        запись инвертированного индекса в файл
        def write_to_file(self, path, post_list):
        """
        with open(path, 'w+', encoding='utf-8') as file:
            for i in post_list:
                inv_index = f'{i}:{post_list[i]}\n'
                file.write(inv_index)

    def combine_blocks(self):
        """
        объединение всех блочных индексов в один
        def combine_blocks(self):
        """

        with open(f'{INDEX_PATH}tf.pickle', 'rb') as tf_file:
            tf = pickle.load(tf_file)
        with open(f'{INDEX_PATH}index.txt', 'w+',
                  encoding='utf-8') as full_index_file:
            # открываем на чтение все файлы, содержащие инвертированные индексы
            spimi_blocks = [open(ARCHIVE_PATH + block, 'r', encoding='utf-8')
                            for block in os.listdir(ARCHIVE_PATH)]
            files_n_indexes = OrderedDict()
            # чтение из каждого блочного файла заданное число строк
            for i, file in tqdm(enumerate(spimi_blocks), ascii=True,
                                desc='get blocks from the all files at time'):
                temp_block = self.get_n_lines_of_file(5000, file)
                # если данные в блоке есть
                if len(temp_block) != 0:
                    files_n_indexes[i] = temp_block
            combined: bool = False
            term_posting_list = OrderedDict()
            while not combined:
                # формирование общего инвертированного индекса для всех блоков
                term_posting_list, max_term = self.get_post_block(
                    files_n_indexes, term_posting_list)
                print('writing to the file...\n')
                item = term_posting_list.popitem(False)
                term = item[0]
                # делим инвертированные индексы для записи в txt файл,
                # как term:[document numbers] термы меньше наименьшего в
                # последних прочтенных строках блока
                while term < max_term:
                    inv_index = f'{term}:{item[1]}\n'
                    full_index_file.write(inv_index)
                    doc_num = len(item[1])
                    # вычисляем tf-idf: tf * общее число документов /
                    # количество файлов, содержащие данный терм
                    for doc_id in tf:
                        try:
                            tf[doc_id][term] = tf[doc_id][
                                                   term] * 3000 / doc_num
                        finally:
                            continue
                    item = term_posting_list.popitem(False)
                    term = item[0]

                files_n_indexes = OrderedDict()
                for i, file in tqdm(enumerate(spimi_blocks), ascii=True,
                                    desc='get blocks from the files at time'):
                    temp_block = self.get_n_lines_of_file(5000, file)
                    if len(temp_block) != 0:
                        files_n_indexes[i] = temp_block
                # если файлы прочтены, то останавливаем цикл и дописываем
                # оставшиеся индексы
                if len(files_n_indexes) == 0:
                    item = term_posting_list.popitem(False)
                    term = item[0]
                    while len(term_posting_list) > 0:
                        inv_index = f'{term}:{item[1]}\n'
                        full_index_file.write(inv_index)
                        doc_num = len(item[1])
                        for doc_id in tf:
                            try:
                                tf[doc_id][term] = tf[doc_id][
                                                       term] * 3000 / doc_num
                            finally:
                                continue
                        item = term_posting_list.popitem(False)
                        term = item[0]
                    combined = True
        # сортируем файл tf-idf по значению tf-idf и записываем в файл
        for doc_id in tf:
            temp_list = list(tf[doc_id].items())
            temp_list.sort(key=lambda tf_score: tf_score[1], reverse=True)
            tf[doc_id] = temp_list
        with open(f'{INDEX_PATH}tf_idf.pickle', 'wb') as file:
            pickle.dump(tf, file)

    @staticmethod
    def get_n_lines_of_file(num, file):
        """
        получение n следующих строк из открытого файла
         def get_n_element_of_file(self, n, file):
        """
        term_post_list = OrderedDict()
        for i, line in enumerate(file):
            inv_index = line[:-1]  # убираем \n
            # делим на терм и список документов
            inv_index = inv_index.split(':')
            term_post_list[inv_index[0]] = ast.literal_eval(
                inv_index[1])
            i += 1
            if i == num:
                break
        return term_post_list

    @staticmethod
    def get_post_block(block_post_list, term_posting_list):
        """
        создание общего инвертированного индекса для блоков
        def get_post_block(self, block_post_list, term_posting_list):
        """
        idx = list(block_post_list.keys())[0]
        last_min_term = list(block_post_list[idx].keys())[-1]
        # нахождение наименьшего терма в последних строках блоков
        for i in block_post_list:
            temp_term = list(block_post_list[i].keys())[-1]
            if last_min_term > temp_term:
                last_min_term = temp_term
        # формирования общего инвертированного индекса
        for block in tqdm(block_post_list, ascii=True,
                          desc='get posting list of the blocks'):
            for term in block_post_list[block]:
                if term not in term_posting_list:
                    term_posting_list[term] = block_post_list[block][term]
                else:
                    term_posting_list[term] += block_post_list[block][term]
        # сортирвоание термов м списка номеров документов
        term_posting_list = OrderedDict(sorted(term_posting_list.items()))
        for term in term_posting_list:
            term_posting_list[term] = sorted(term_posting_list[term])
        return term_posting_list, last_min_term

    @staticmethod
    def make_pickle_file():
        """
        храние индексов в формате pickle
        def make_pickle_file(self):
        """

        with open(f'{INDEX_PATH}index.txt', 'r') as file:
            term_post_list = OrderedDict()
            for line in file:
                inv_index = line[:-1]  # убираем \n
                # делим на терм и список документов
                inv_index = inv_index.split(':')
                term_post_list[inv_index[0]] = ast.literal_eval(
                    inv_index[1])
        with open(f'{INDEX_PATH}index.pickle', 'wb') as file:
            pickle.dump(term_post_list, file)
