from preprocess import get_terms
from collections import OrderedDict
import os
import pickle
from tqdm import tqdm
import re

# устанавливаем лимит 1MB для предотвращения переполнения памяти
memory_size = 1024 * 1024
# путь для прочтения корпуса
dir_path = "docs_data/"
# путь для хранения промежуточных файлов блочного индексирования
archive_path = "old_archive/"
# путь для сохранения объединенного индекса
index_path = "index/"


class GenerateIndex:
    """
    класс содержит облегченные методы реализации SPIMI индексирования
    """

    def __init__(self):
        """
        инициализация класса, создания директорий
        def __init__(self):
         """
        if not os.path.exists(archive_path):
            os.mkdir(archive_path)
        if not os.path.exists(index_path):
            os.mkdir(index_path)

    def bloks_splitting(self):
        """
        создание промежуточных файлов блочного индексирования
        def bloks_splitting(self):
        """

        doc_list = os.listdir(dir_path)
        block_name = 0
        doc_block = []
        block_size = 0
        term_postings_list = {}
        # прочитываем все файлы в корпуе
        for doc in tqdm(doc_list, ascii=True, desc=f'indexing{block_name}'):
            doc_path = f'{dir_path}{doc}'
            # собираем список имен документов не превышающих лимита
            doc_block.append(doc)
            block_size += os.path.getsize(doc_path)

            if block_size > memory_size:
                term_postings_list = {}
                # для каждого документа из собранного блока запускаем
                # токенизацию и лемматизацию термов
                for doc_b in doc_block:
                    doc_path = f'{dir_path}{doc_b}'
                    terms = get_terms(doc_path)
                    # id из названия
                    document_id = int(re.search(r'\d+', doc_b)[0])
                    # проверяем существует ли уже терм в словаре
                    for term in terms:
                        if term not in term_postings_list:
                            term_postings_list[term] = [document_id]
                        # если этого индекса нет в posting list
                        elif document_id not in term_postings_list[term]:
                            term_postings_list[term].append(document_id)
                # сохраняем промежуточный блочный индекс
                with open(f'{archive_path}{block_name}.pickle', 'wb') as f:
                    pickle.dump(term_postings_list, f)
                block_name += 1
                term_postings_list = {}
                block_size = 0
                doc_block = []

    def combine_blocks(self):
        """
        объединение всех блочных индексов в один
        def combine_blocks(self):
        """
        doc_list = os.listdir(archive_path)
        full_index = OrderedDict()
        for doc in tqdm(doc_list, ascii=True, desc='union blocks'):
            doc_path = f'{archive_path}{doc}'
            with open(doc_path, 'rb') as f:
                index = pickle.load(f)
            # os.remove(doc_path)
            # проверка на существования терма и добавление при отсутствии
            for term in index.keys():
                if term not in full_index:
                    full_index[term] = index[term]
                else:
                    full_index[term] += index[term]
        # сортировка термов
        full_index = OrderedDict(sorted(full_index.items()))
        # сортировка списка документов
        for term in full_index:
            full_index[term] = sorted(full_index[term])
        # сохранение файла
        with open(f'{index_path}index.pickle', 'wb') as f:
            pickle.dump(full_index, f)
