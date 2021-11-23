import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')


def get_query_terms(query_list):
    """
    возвращает массив лемматизированных термов запроса
    def get_query_terms(query_list):
    """
    token_list = []
    lemmatizer = WordNetLemmatizer()
    for token in query_list:
        # преобразуем слова, но не операторы
        if token.lower() not in ['and', 'or', 'not']:
            token_list.append(lemmatizer.lemmatize(token))
        else:
            token_list.append(token)
    return token_list


def get_terms(doc_path):
    """
    возвращает массив токенизированных и лемматизированных термов
    def get_terms(doc_path)
    """
    stop_words = stopwords.words('english')
    lemmatizer = WordNetLemmatizer()
    with open(doc_path, 'r', encoding='utf-8') as file:
        doc = file.readlines()
    terms = []
    for line in doc:
        # удаляем в строке нелатинские буквы и символы пунктуации,
        # кроме '-', цифры тоже остаются
        line = re.sub("[^A-Za-z0-9-]", " ", line)
        # разбиваем строку на термы
        tokens = nltk.word_tokenize(line)
        # удаляем стоп-слова
        filtered_words = [word for word in tokens if
                          word not in stop_words]
        # лемматизируем термы
        terms += [lemmatizer.lemmatize(word) for word in filtered_words]
    return terms
