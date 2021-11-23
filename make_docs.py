import os
from tqdm import tqdm


def get_corpus():
    """
    чтение файла-корпуса и разбиение его на документы
    def get_corpus():
    """

    dir_path = "docs_data/"

    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    with open('corpus/wiki_corpus.txt', 'r', encoding='utf-8') as common_file:
        for ind in tqdm(range(3000), ascii=True, desc='get corpus'):
            with open(f'{dir_path}/{ind}.txt', 'w+', encoding='utf-8') as file:
                file.write(common_file.readline())
