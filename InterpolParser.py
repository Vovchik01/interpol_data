import concurrent.futures
import json
import os
import re
import operator
import requests
from bs4 import BeautifulSoup
from Parser import Parser


class InterpolParser(Parser):
    """
    Класс собирающий данные о разыскиваемых
    на сайте интерпола (https://www.interpol.int/How-we-work/Notices/View-Red-Notices).
    """

    BASE_DIR_FOR_DATA = f'.{os.sep}all_data{os.sep}'
    BASE_URL = 'https://www.interpol.int/How-we-work/Notices/View-Red-Notices'
    BASE_JSON_RESPONSE_URL = 'https://ws-public.interpol.int/notices/v1/red'
    MAX_SEARCH_RESULT_DISPLAY = 160
    MAX_SEARCH_RESULT_PER_PAGE = 20
    MIN_REDNOTICE_AGE = 17
    MAX_REDNOTICE_AGE = 100

    def __init__(self, max_threads: int):
        super().__init__(max_threads=max_threads)

    @staticmethod
    def get_rednotice_search_result_number(page_data: dict):
        """
        Получает количество разыскиваемых по конкретному запросу.
        Ответ каждой страницы текст json формата.
        Total является общим значением найденного по фильтрации.
        :param page_data: Словарь с данными о странице поиска.
        :return: Целое число.
        """
        try:
            total_number = int(page_data['total'])
            return total_number
        except Exception as ex:
            print('get_rednotice_search_result_number', ex)
            return 0

    def __get_all_country_codes(self):
        """
        Находит список всех стран и их коды, для дальнейшей возможности фильтрации.
        Так как список стран с большой вероятностью меняться не будет, после первого запроса он сохраняется в файл
        и в дальнейшем берется оттуда.
        :return: Словарь со всеми доступными для выбора странами. {'Название страны':'Код страны'}
        """
        file_name = 'countries'
        if os.path.exists(f'{self.BASE_DIR_FOR_DATA}{file_name}.json'):
            with open(f'{self.BASE_DIR_FOR_DATA}{file_name}.json', encoding='utf-8') as countries:
                return json.load(countries)
        else:
            page_data = self.get_page(self.BASE_URL)
            soup = BeautifulSoup(page_data, 'lxml')
            left_column = soup.find('div', class_='twoColumns__leftColumn')
            select_nationality = left_column.find('select', id='nationality')
            all_codes = select_nationality.find_all('option')
            country_codes = {}
            for code in all_codes[1:]:
                country_name = code.text
                country_code = code.get('value')
                country_codes[country_name] = country_code
            self.write_data_into_json(self.BASE_DIR_FOR_DATA, file_name, country_codes)
            return country_codes

    def get_all_country_urls(self):
        """
        Составляет ссылки для запросов по каждой стране.
        :return: Список строк.
        """
        url_postfix = '?=&nationality='
        urls_list = [f'{self.BASE_JSON_RESPONSE_URL}{url_postfix}{code}' for code in
                     self.__get_all_country_codes().values()]
        return urls_list

    def check_for_acceptability(self, url: str, oper: operator, condition: int):
        """
        Проверка количества результатов поиска на то подходит ли оно под определённое условие.
        Частый случай - меньше ли количество чем 160. (160 - это максимум результатов, который выдаёт сайт по запросу)
        :param url: Ссылка запроса.
        :param oper: Оператор условия.
        :param condition: Условие для сравнения.
        :return: Возвращает True, если условие выполняется.
        """
        page = self.get_json_page(url)
        result_number = self.get_rednotice_search_result_number(page)
        print(f'{url} has {result_number} results')  # debugging line
        if oper(result_number, condition):
            return True

    @staticmethod
    def get_urls_with_gender_filter(base_url: str):
        """
        Составляет ссылки запроса с фильтром по принадлежности к определённому полу.
        :param base_url: Ссылка к которой будет добавляться фильтр.
        :return: Список строк.
        """
        url_postfix = '&sexId='
        genders_codes = ['F', 'M', 'U']  # Female, Male, Unknown
        urls_with_gender_filter = [f'{base_url}{url_postfix}{i}' for i in genders_codes]
        return urls_with_gender_filter

    @staticmethod
    def get_urls_with_age_filter(base_url: str, age_ranges: list):
        """
        Составляет ссылки запроса с фильтром по возрастному ограничению.
        :param base_url: Ссылка к которой будет добавляться фильтр.
        :param age_ranges: Список с парами значений (минимальный и максимальный возраст).
        :return: Список строк.
        """
        url_postfix_min = '&ageMin='
        url_postfix_max = '&ageMax='
        urls_with_gender_filter = [f'{base_url}{url_postfix_min}{i[0]}{url_postfix_max}{i[1]}' for i in age_ranges]
        return urls_with_gender_filter

    def separate_url(self, country_url: str):
        """
        Проверяет количество полученных по запросу результатов и если необходимо,
        разделяет запрос с фильтром только по стране, на более точные с добавлением пола, возраста.
        Разделение необходимо для возможности получить все данные с сайта,
        так как максимальная выдача по запросу 160 результатов,
        а некоторые общие запросы подразумевают получение большего количества.
        :param country_url: Ссылка общего запроса.
        :return: Список строк.
        """

        urls_list = set()
        if self.check_for_acceptability(country_url, operator.le, self.MAX_SEARCH_RESULT_DISPLAY):
            urls_list.add(country_url)
            return list(urls_list)
        else:
            urls_with_gender_filter = self.get_urls_with_gender_filter(country_url)
            for url_with_gender_filter in urls_with_gender_filter:
                if self.check_for_acceptability(url_with_gender_filter, operator.le, self.MAX_SEARCH_RESULT_DISPLAY):
                    urls_list.add(url_with_gender_filter)
                else:
                    urls_with_age_filter = self.get_urls_with_age_filter(url_with_gender_filter,
                                                                         age_ranges=[(self.MIN_REDNOTICE_AGE, 25),
                                                                                     (26, 29), (30, 34), (35, 38),
                                                                                     (39, 40), (41, 55), (56, 65),
                                                                                     (66, self.MAX_REDNOTICE_AGE)])
                    for url_with_age_filter in urls_with_age_filter:
                        if self.check_for_acceptability(url_with_age_filter, operator.le,
                                                        self.MAX_SEARCH_RESULT_DISPLAY):
                            urls_list.add(url_with_age_filter)
                        else:
                            start_range, end_range = map(int, re.findall(r"(\d\d)", url_with_age_filter))
                            frequent_urls_with_age_filter = self.get_urls_with_age_filter(url_with_gender_filter,
                                                                                          [(i, i) for i in
                                                                                           range(start_range,
                                                                                                 end_range + 1)])
                            for url in frequent_urls_with_age_filter:
                                if self.check_for_acceptability(url, operator.gt, 0):
                                    urls_list.add(url)
            return list(urls_list)

    def get_full_search_result_page_url(self, search_url: str, search_result_number: int):
        """
        Составляет ссылку на полную страницу найденных данных.
        Чтобы не бегать по ссылкам каждой страницы можно сразу получить все найденные результаты.
        (Если результатов <= 160 само собой.)
        :param search_url: Ссылка запроса по стране или по стране и возрасту.
        :param search_result_number: Количество найденных.
        :return: Строка.
        """
        if search_result_number <= self.MAX_SEARCH_RESULT_PER_PAGE:
            # print(search_url, search_result_number)  # debugging line
            return search_url
        else:
            full_search_result_page_url = f'{search_url}&resultPerPage={self.MAX_SEARCH_RESULT_DISPLAY}'
            # print(search_url, search_result_number)  # debugging line
            return full_search_result_page_url

    @staticmethod
    def __get_rednotice_url(page_data: dict, order_number: int):
        """
        Забирает ссылку на разыскиваемого с общей страницы поиска.
        :param page_data: Словарь с данными страницы.
        :param order_number: Номер по порядку.
        В получаемых данных у каждого разыскиваемого есть id от 0 до найденного максимума -1.
        :return: Строка.
        """
        try:
            rednotice_url = str(page_data['_embedded']['notices'][order_number]['_links']['self']['href'])
            return rednotice_url
        except Exception as ex:
            print(f'__get_rednotice_url {ex}')
            return 0

    def get_rednotice_urls(self, full_page: dict):
        """
        Забирает все ссылки на разыскиваемых со страницы.
        :param full_page: Словарь с данными страницы.
        :return: Список строк.
        """
        result_number = self.get_rednotice_search_result_number(full_page)
        # print(f'Ссылок для сбора: {result_number}')  # debugging line
        if result_number > 0:
            rednotice_urls = set()
            for i in range(0, result_number):
                url = self.__get_rednotice_url(full_page, i)
                # print(f'Беру ссылку: #{i} - {url}')  # debugging line
                if url == 0:
                    continue
                else:
                    rednotice_urls.add(url)
            # print(f'Собрано ссылок: {len(rednotice_urls)}')  # debugging line
            return list(rednotice_urls)
        else:
            return list()

    def get_full_page(self, page_url: str):
        """
        Возвращает ответ полной страницы запроса.
        :param page_url: Ссылка на конкретный запрос.
        :return: Словарь с данными страницы.
        """
        page_data = self.get_json_page(page_url)
        search_result_number = self.get_rednotice_search_result_number(page_data)
        full_page_url = self.get_full_search_result_page_url(page_url, search_result_number)
        full_page = self.get_json_page(full_page_url)
        return full_page

    def get_all_prepared_country_urls(self):
        """
        Подготавливает все необходимые ссылки запросов фильтров.
        :return: Список строк.
        """

        file_name = 'all_prepared_country_urls'
        all_prepared_country_urls = set()
        if os.path.exists(f'{self.BASE_DIR_FOR_DATA}{file_name}.csv'):
            all_prepared_country_urls = \
                set(self.read_data_from_csv_to_list(f'{self.BASE_DIR_FOR_DATA}{file_name}.csv'))
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                all_raw_urls = self.get_all_country_urls()
                for url in executor.map(self.separate_url, all_raw_urls):
                    all_prepared_country_urls.update(url)
            self.write_data_into_csv(self.BASE_DIR_FOR_DATA, file_name, all_prepared_country_urls)
        return list(all_prepared_country_urls)

    def get_all_rednotice_urls(self):
        """
        Подготавливает ссылки на станицу каждого разыскиваемого на сайте.
        :return: Список строк.
        """

        file_name = 'all_collected_rednotice_urls'
        all_rednotice_urls = set()
        if os.path.exists(f'{self.BASE_DIR_FOR_DATA}{file_name}.csv'):
            all_rednotice_urls = \
                set(self.read_data_from_csv_to_list(f'{self.BASE_DIR_FOR_DATA}{file_name}.csv'))
        else:
            country_urls = self.get_all_prepared_country_urls()
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                for page in executor.map(self.get_full_page, country_urls):
                    urls = self.get_rednotice_urls(page)
                    all_rednotice_urls.update(urls)
            self.write_data_into_csv(self.BASE_DIR_FOR_DATA, file_name, all_rednotice_urls)
        return list(all_rednotice_urls)

    def get_images_links(self, red_notice_data: dict):
        """
        Собирает все имеющиеся ссылки изображений на странице.
        :param red_notice_data: Словарь ответ сервера.
        :return: Список строк.
        """
        try:
            base_images_link = red_notice_data['_links']['images']['href']
            images_links = self.get_json_page(base_images_link)
            images_links_list = images_links['_embedded']['images']
            clean_links = []
            if len(images_links_list) == 0:
                return []
            else:
                for link in images_links_list:
                    clean_links.append(link['_links']['self']['href'])
                return clean_links
        except Exception as ex:
            print(f'get_images_link - {ex}')
            return []

    def save_rednotice_images(self, image_urls: list, rednotice_clean_data: dict):
        """
        Загружает и сохраняет изображения на определенного разыскиваемого.
        Путь сохранения: './all_data/название_страны/имя_разыскиваемого/'
        Название файла: 'имя_разыскиваемого.png'
        :param image_urls: Ссылки на изображения.
        :param rednotice_clean_data: Подготовленные данные о разыскиваемом.
        :return: Список строк.
        """

        full_path_to_file = []
        try:
            rednotice_name = self.get_rednotice_full_name(rednotice_clean_data)
            file_path = self.get_path(rednotice_clean_data)
            for index, url in enumerate(image_urls):
                data = requests.get(url).content
                full_path_to_file.append(self.save_image(file_path, f'{rednotice_name}_{index+1}', data))
            return full_path_to_file
        except Exception as ex:
            print(f'save_rednotice_images - {ex}')

    @staticmethod
    def get_clean_dict_value(rednotice_clean_data: dict, key: str):
        """
        Проверяет наличие, и возвращает значение из словаря по заданному ключу.
        :param rednotice_clean_data: Подготовленные данные о разыскиваемом.
        :param key: Ключ.
        :return: Строка - значение.
        """
        try:
            if rednotice_clean_data.get(key) is None:
                if key == 'nationalities':
                    key = 'country'
                value = f'Unknown_{key}'
            elif isinstance(rednotice_clean_data.get(key), list):
                value = rednotice_clean_data.get(key)[0].strip().replace(' ', '_').replace('/', '_')
            else:
                value = rednotice_clean_data.get(key).strip().replace(' ', '_').replace('/', '_')
            return value
        except Exception as ex:
            print('get_clean_dict_value - ', ex)

    def get_rednotice_full_name(self, rednotice_clean_data: dict):
        """
        Собирает полное имя разыскиваемого для дальнейшего использования.
        :param rednotice_clean_data: Подготовленные данные о разыскиваемом.
        :return: Строка.
        """
        try:
            name = self.get_clean_dict_value(rednotice_clean_data, 'name')
            forename = self.get_clean_dict_value(rednotice_clean_data, 'forename')
            entity_id = self.get_clean_dict_value(rednotice_clean_data, 'entity_id')
            rednotice_name = f'{name}_{forename}_{entity_id}'
            return rednotice_name
        except Exception as ex:
            print(f'get_full_name - {ex}')

    def get_path(self, rednotice_clean_data: dict):
        """
        Собирает путь к папке сохранения данных о разыскиваемом.
        :param rednotice_clean_data: Подготовленные данные о разыскиваемом.
        :return: Строка.
        """
        try:
            country_name = self.get_country_name(self.get_clean_dict_value(rednotice_clean_data, 'nationalities'))
            rednotice_name = self.get_rednotice_full_name(rednotice_clean_data)
            file_path = f'{self.BASE_DIR_FOR_DATA}{country_name}{os.sep}{rednotice_name}{os.sep}'
            return file_path
        except Exception as ex:
            print(f'get_path - {ex}')

    @staticmethod
    def get_rednotice_clean_data(rednotice_data: dict):
        """
        Очищает словарь с данными о разыскиваемом от лишних и пустых значений.
        :param rednotice_data: Словарь данных.
        :return: Словарь подготовленных данных.
        """
        try:
            del rednotice_data['_embedded']
            del rednotice_data['_links']
            filtered = {k: v for k, v in rednotice_data.items() if v is not None}
            return filtered
        except Exception as ex:
            print(f'get_rednotice_clean_data - {ex}')

    def get_country_name(self, country_code):
        """
        Получает название страны по её коду.
        :param country_code: Код страны.
        :return: Строка - название страны.
        """
        countries = self.__get_all_country_codes()
        for k, v in countries.items():
            if v == country_code:
                return k

    def write_data_into_file(self, rednotice_clean_data: dict):
        """
        Записывает данные о разыскиваемом в json файл.
        :param rednotice_clean_data: Подготовленные данные о разыскиваемом.
        :return: Строка - путь к файлу
        """
        try:
            rednotice_name = self.get_rednotice_full_name(rednotice_clean_data)
            file_path = self.get_path(rednotice_clean_data)
            full_path_to_file = self.write_data_into_json(file_path, rednotice_name, rednotice_clean_data)
            return full_path_to_file
        except Exception as ex:
            print(f'write_data_into_file - {ex}')

    def get_rednotice_data(self, rednotice_url: str):
        """
        Получение всех найденных данных о разыскиваемом. Сохранение файла и изображений в определенный каталог.
        :param rednotice_url: Ссылка на страницу разыскиваемого.
        :return: Словарь данных.
        """
        print(rednotice_url)
        red_notice_data = self.get_json_page(rednotice_url)
        images_urls = self.get_images_links(red_notice_data)
        clean_rednotice_data = self.get_rednotice_clean_data(red_notice_data)
        self.write_data_into_file(clean_rednotice_data)
        self.save_rednotice_images(images_urls, clean_rednotice_data)
        return red_notice_data

    def get_all_rednotice_data(self):
        """
        Прогонка всех ссылок через функцию получения данных.
        :return: None.
        """
        all_rednotice_urls = self.get_all_rednotice_urls()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            executor.map(self.get_rednotice_data, all_rednotice_urls)
