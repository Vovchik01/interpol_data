import csv
import json
import os
import requests


class Parser:
    """
    Базовый класс для парсинга.
    """
    HEADERS = {
        'Accept':
            'text/html, application/xhtml+xml, application/xml; q=0.9, image/avif, image/webp, */*; q=0.8',
        'Accept-Encoding':
            'gzip, deflate, br',
        'Accept-Language':
            'ru-RU, ru; q=0.8, en-US; q=0.5, en; q=0.3',
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv: 99.0) Gecko/20100101 Firefox/99.0'
    }
    URL_REQUEST_COUNTER = 0

    def __init__(self, max_threads: int):
        self.max_threads = max_threads

    def get_page(self, url: str):
        """
        Делает запрос по заданному url.
        :param url: URL адрес.
        :return: Строка ответ.
        """
        try:
            result = requests.get(url=url, headers=self.HEADERS)
            if not result.ok:
                print(f'get_page - {url}', result)
            self.URL_REQUEST_COUNTER += 1
            return result.text
        except Exception as ex:
            print(ex)
            return f'Не удалось обратиться к странице - {url}'

    def get_json_page(self, url: str):
        """
        Делает запрос по заданному url.
        :param url: URL адрес.
        :return: JSON строка преобразованная в словарь.
        """
        try:
            result = json.loads(self.get_page(url))
            return result
        except Exception as ex:
            print(ex, f'{url} - Полученный ответ не в формате json.')
            return {}

    @staticmethod
    def check_and_create_path(path):
        """
        Проверяет наличие и если необходимо, создает папки, по указанному пути.
        Заменяет возможные ошибки в разделителях.
        :param path: Строка пути, который необходимо проверить.
        :return: Строка созданного пути.
        """
        file_path = path.replace('/', os.sep).replace('\\', os.sep).replace('//', os.sep).replace('\\\\', os.sep)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        return file_path

    def write_data_into_json(self, file_path: str, file_name: str, data):
        """
        Записывает собранные данные в файл формата json.
        :param file_path: Путь к файлу.
        :param file_name: Желаемое имя файла без разширения.
        :param data: Словарь или список словарей.
        :return: Строка с относительной ссылкой на сохраненный json файл.
        """

        self.check_and_create_path(file_path)

        full_file_path = f'{file_path}{os.sep}{file_name}.json'
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception as ex:
                print(ex, 'Невозможно преобразовать строку в словарь. \n '
                          'Может помочь заменить двойные внешние кавычки на одинарные.')
        if not os.path.exists(full_file_path):
            with open(full_file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
        else:
            print(f'Файл "{full_file_path}" уже существует.')

        return full_file_path

    def save_image(self, file_path, file_name, data):
        """
        Сохраняет изображения в формат .png в указанный каталог.
        :param file_path: Путь к файлу.
        :param file_name: Желаемое имя файла без разширения.
        :param data: Список собранных данных.
        :return: Строка с относительной ссылкой на изображение.
        """

        self.check_and_create_path(file_path)

        full_file_path = f'{file_path}{os.sep}{file_name}.png'
        if not os.path.exists(full_file_path):
            with open(full_file_path, 'wb') as file:
                file.write(data)
        else:
            print(f'Файл "{full_file_path}" уже существует.')

        return full_file_path

    def write_data_into_csv(self, file_path, file_name, data):
        """
        Записывает собранные данные в файл формата csv.
        :param file_path: Путь к файлу.
        :param file_name: Желаемое имя файла без разширения.
        :param data: Список собранных данных.
        :return: Строка с относительной ссылкой на csv файл.
        """

        self.check_and_create_path(file_path)

        full_file_path = f'{file_path}{os.sep}{file_name}.csv'
        if not os.path.exists(full_file_path):
            with open(full_file_path, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(zip(data))
        else:
            print(f'Файл "{full_file_path}" уже существует.')

        return full_file_path

    @staticmethod
    def read_data_from_csv_to_list(file_path):
        """
        Считывает данные из csv файла.
        :param file_path: Путь к файлу.
        :return: Список прочитанных строк.
        """

        if not os.path.exists(file_path):
            print(file_path, 'Неверный путь, либо такого файла не существует')
            return []
        else:
            result_list = []
            with open(file_path, 'r', encoding='utf-8', newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    result_list.append(*row)
            return result_list
