import pickle
from generate_index import GenerateIndex as Gi
# from old_version_generate_index import GenerateIndex as OldGi
import make_docs
from rank import rank
from bool_search import Query

DIR_PATH = "docs_data/"

if __name__ == '__main__':
    # создание корпуса документов
    # make_docs.get_corpus()

    # реализация обратного индекса, старая версия
    # OldGi().bloks_splitting()
    # OldGi().combine_blocks()

    # реализация обратного индекса, новая версия
    # Gi().blocks_splitting()
    # Gi().combine_blocks()
    # Gi().make_pickle_file()

    # чтение с диска индексов
    with open('index/index.pickle', 'rb') as file:
        index = pickle.load(file)

    while True:
        print('\n\nВведите запрос для поиска:')
        QUERY_STRING = str(input('>'))
        if QUERY_STRING == '-1':
            break
        query_list = QUERY_STRING.split()
        # производим поиск разбитого по словам запроса
        result = Query().process_query(query_list, index)
        print(f'{len(result)} найдено. Первые 10 документов:{result[:10]}')

        if len(result) > 0:
            result = rank(query_list, result)
            print(f'{len(result)} найдено. Первые 10 ранжированных '
                  f'документов:{result[:10]}')
            for doc in result[:10]:
                PRINT_STRING = '_' * 50 + '\n'
                with open(f'{DIR_PATH}{doc}.txt', 'r', encoding='utf-8') as f:
                    for i in range(3):
                        PRINT_STRING += f.readline()[:100]
                print(PRINT_STRING)

        print("\n\nВведите '-1' для завершения поиска:")

        # вывод первых 3 результатов в консоль
        # for doc in result[:3]:
        #     print_string = '_' * 50 + '\n'
        #     with open(f'{dir_path}{doc}.txt', 'r', encoding='utf-8') as f:
        #         for i in range(3):
        #             print_string += f.readline()[:100]
        #     print(print_string)
