import sqlite3
import requests
import bs4
import random
import datetime
import time
import re
import operator
from urllib.parse import urlparse
import pandas as pd
import matplotlib.pyplot as plt

wordMetrics = []
linkBetweenMetrics = []
ignorewords = {'в', 'без', 'до', 'из', 'к', 'на', 'по', 'о', 'от', 'перед', 'при', 'через', 'с', 'у', 'за', 'над', 'об',
               'под', 'про', 'для', ''}
class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        print("Конструктор")
        self.connection = sqlite3.connect(dbFileName)
        pass

    # 0. Деструктор
    def __del__(self):
        print("Деструктор")
        self.connection.commit()
        self.connection.close()
        pass

    # 7. Инициализация таблиц в БД
    def initDB(self):
        print("Создать пустые таблицы с необходимой структурой")

        curs = self.connection.cursor()

        # 1. Таблица wordlist -----------------------------------------------------
        # Удалить таблицу wordlist из БД
        sqlDropWordlist = """DROP TABLE   IF EXISTS    wordlist;  """
        print(sqlDropWordlist)
        curs.execute(sqlDropWordlist)

        # Сформировать SQL запрос
        sqlCreateWordlist = """
            CREATE TABLE   IF NOT EXISTS   wordlist (
                rowid  INTEGER   PRIMARY KEY   AUTOINCREMENT, -- первичный ключ
                word TEXT   NOT NULL, -- слово
                isFiltred INTEGER     -- флаг фильтрации
            );
        """
        print(sqlCreateWordlist)
        curs.execute(sqlCreateWordlist)

        # 2. Таблица URLList -------------------------------------------------------
        sqlDropURLlist = """DROP TABLE   IF EXISTS    URLList;  """
        print(sqlDropURLlist)
        curs.execute(sqlDropURLlist)

        sqlCreateURLlist = """CREATE TABLE URLList (
                rowid	INTEGER,
                URL	TEXT NOT NULL,
                PRIMARY KEY(rowid AUTOINCREMENT)
            ); """
        print(sqlCreateURLlist)
        curs.execute(sqlCreateURLlist)

        # 3. Таблица wordlocation ----------------------------------------------------
        sqlDropwordlocation = """DROP TABLE   IF EXISTS    wordlocation;  """
        print(sqlDropwordlocation)
        curs.execute(sqlDropwordlocation)

        # Сформировать SQL запрос
        sqlCreatewordlocation = """CREATE TABLE wordlocation (
                rowid	INTEGER,
                fk_wordid	INTEGER NOT NULL,
                fk_URLid	INTEGER NOT NULL,
                location	INTEGER NOT NULL,
                PRIMARY KEY(rowid AUTOINCREMENT)
                FOREIGN KEY (fk_wordId) REFERENCES wordList (wordId),
                FOREIGN KEY (fk_urlId) REFERENCES urlList (urlId)
            ); """
        print(sqlCreatewordlocation)
        curs.execute(sqlCreatewordlocation)
        # 4. Таблица linkbeetwenurl --------------------------------------------------
        sqlDroplinkbeetwenURL = """DROP TABLE   IF EXISTS    linkbeetwenURL;  """
        print(sqlDroplinkbeetwenURL)
        curs.execute(sqlDroplinkbeetwenURL)

        # Сформировать SQL запрос
        sqlCreatelinkbeetwenURL = """CREATE TABLE linkbeetwenURL (
                rowid	INTEGER,
                fk_fromURL_id	INTEGER NOT NULL,
                fk_ToURL_id	INTEGER NOT NULL,
                PRIMARY KEY(rowid AUTOINCREMENT),
                FOREIGN KEY (fk_fromURL_id) REFERENCES URLList (urlId),
                FOREIGN KEY (fk_ToURL_id) REFERENCES URLList (urlId)
            ); """
        print(sqlCreatelinkbeetwenURL)
        curs.execute(sqlCreatelinkbeetwenURL)
        # 5. Таблица linkwords -------------------------------------------------------
        sqlDroplinkwords = """DROP TABLE   IF EXISTS    linkwords;  """
        print(sqlDroplinkwords)
        curs.execute(sqlDroplinkwords)

        # Сформировать SQL запрос
        sqlCreatelinkwords = """CREATE TABLE linkwords (
                rowid	INTEGER,
                fk_wordid	INTEGER NOT NULL,
                fk_linkid	INTEGER NOT NULL,
                PRIMARY KEY(rowid AUTOINCREMENT)
                FOREIGN KEY (fk_wordId)  REFERENCES wordList (wordId),
	            FOREIGN KEY (fk_linkId)  REFERENCES linkBetweenURL (linkId)
            ); """
        print(sqlCreatelinkwords)
        curs.execute(sqlCreatelinkwords)
        pass

    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=2):


        for currDepth in range(maxDepth):

            print("===========Глубина обхода ", currDepth, "=====================================")
            counter = 0  # счетчик обработанных страниц
            nextUrlSet = set()  # создание Множество(Set) следующих к обходу элементов

            # Вар.1. обход каждого url на текущей глубине
            # for url in  urlList:
            # шаг-1. Выбрать url-адрес для обработки

            # Вар.2. обход НЕСКОЛЬКИХ url на текущей глубине
            for num in range(0, 5):

                # шаг-1. Выбрать url-адрес для обработки
                numUrl = random.randint(0, len(urlList) - 1)  # назначить номер элемента в списке urlList
                url = urlList[numUrl]  # получить url-адрес из списка
                print(numUrl)
                counter += 1
                curentTime = datetime.datetime.now().time()

                try:
                    print("{}/{} {} Попытка открыть {} ...".format(counter, len(urlList), curentTime, url))
                    # шаг-2. Запрашивать HTML-код
                    html_doc = requests.get(url).text  # получить HTML код страницы

                except Exception as e:
                    # обработка исключений при ошибке запроса содержимого страницы
                    print(e)
                    continue  # перейти к следующему url

                # шаг-3. Разобрать HTML-код на составляющие
                soup = bs4.BeautifulSoup(html_doc, "html.parser")
                title = soup.find('title')
                if soup.title == None: continue
                print(" ", soup.title.text)

                # шаг-4. Найти на странице блоки со скриптами и стилями оформления ('script', 'style'),
                # а также забаненныее твиттер и фейсбук
                listUnwantedItems = ['script', 'style', 'http://www.facebook.com','https://www.facebook.com',
                                     'http://twitter.com','https://twitter.com']
                for script in soup.find_all(listUnwantedItems):
                    script.decompose()  # очистить содержимое элемента и удалить его из дерева

                # шаг-5. Добавить содержимого страницы в Индекс
                self.addIndex(soup, url)

                # шаг-6. Извлечь с данный страницы инф о ссылка на внешние узлы = получить все тэги <a> = получить все ссылки

                # linksOnCurrentPage = получить все тэги <a>
                linksOnCurrentPage = soup.find_all('a')

                # Обработать каждую ссылку <a>
                for tagA in linksOnCurrentPage:

                    # Проверить начиличе атрибута 'href' у тега <a> (атрибуты находятся в структуре Cловарь Dictionary)
                    if ('href' in tagA.attrs):  # атрибут href задаёт адрес документа
                        # Проверка соответвия ссылок ВАШИМ требованиям
                        nextUrl = tagA.attrs['href']

                        # Выбор "подходящих" ссылок. Вариант: если ссылка начинается с "http"
                        if nextUrl.startswith("http"):
                            print("Ссылка    подходящая ", nextUrl)
                            nextUrlSet.add(nextUrl)
                            urlText = self.gettextonly(tagA)
                            self.addLinkRef(url, nextUrl, urlText)
                            curs = self.connection.cursor()


                            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                            # добавить инф о ссылке в БД  -  addLinkRef(  url,  nextUrl)
                            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                        else:
                            print("Ссылка не подходящая ", nextUrl)
                            pass
                    else:
                        # атрибут href отсутствует
                        continue
                #занесение значений для замеров заполнения таблицы wordList и linkbeetwenURL
                countWord = curs.execute('SELECT COUNT(rowid) FROM wordList;')  # здесь работает нормально
                wordMetrics.append(countWord.fetchone())  # для wordlist-a
                countUrl = curs.execute('SELECT COUNT(rowid) FROM linkbeetwenURL;')
                linkBetweenMetrics.append(countUrl.fetchone())

                # конец цикла для обработки тега <a>

            #фиксируем изменения в БД
            self.connection.commit()
            # шаг-8. Добавить найденые ссылки на другие страницы в "Список очередных сслылок к обходу"
            urlList = list(nextUrlSet)
            # конец обработки всех URL на данной глубине

        pass


    # 1. Индексирование одной страницы
    def addIndex(self, soup, url):
        wordList = list()
        if self.isIndexed(url): return
        print('Индексируется ' + url)
        # Получить список слов
        text = self.gettextonly(soup)
        words = self.separatewords(text)
        # Получить идентификатор URL
        urlid = self.getentryid('URLList', 'url', url)
        # Связать каждое слово с этим URL
        for i in range(len(words)):
            word = words[i]
            #if re.search(r'[a-zA-Z0-9]', word) or word == '':
            if re.search(r'[A-Za-z0-9]', word) or word in ignorewords: # \w = [A-Za-z0-9_]
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            wordList.append(word)
            # commitCounter = 0
            self.connection.execute("insert into wordlocation(fk_URLid,fk_wordid,location) \
             values (%d,%d,%d)" % (urlid, wordid, i))

    # 2. Разбиение текста на слова
    def gettextonly(self, soup):
        # #v = soup.string
        # v = soup.text
        # if v == None:
        #     c = soup.contents
        #     resulttext = ''
        #     for t in c:
        #         subtext = self.gettextonly(t)
        #         resulttext += subtext + '\n'
        #     return resulttext
        # else:
        #     return v.strip()

        result = soup.text.lower()
        return result

    # 3. Разбиение текста на слова
    def separatewords(self, text):
        # splitter = re.compile('\\W*')
        # res = splitter.split(text)
        # #return [s.lower() for s in splitter.split(text) if s != '']
        # return res

        Wordsist = list()
        r1 = re.compile('\\W+')
        Wordsist = r1.split(text)

        # for index in range(0, len(Wordsist)):
        #     if Wordsist[index] != "":
        #         result.append(Wordsist[index])
        # print("Разбиение текста на слова", Wordsist)
        return Wordsist

        # 4. Проиндексирован ли URL

    def isIndexed(self, url):
        # проверить, присутствует ли url в БД  (Таблица urllist в БД)
        u = self.connection.execute("SELECT rowid FROM URLList WHERE URL='%s'" % url).fetchone()
        if u != None:
            v = self.connection.execute(
                """SELECT * FROM wordLocation WHERE fk_URLid=%d""" % u[0]).fetchone()
            if v != None:
                return True
        return False

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):

        cur = self.connection.cursor()

        # Добавление в БД связи между ссылками
        sqlInsert = """ INSERT INTO linkbeetwenURL ( fk_fromURL_id, fk_ToURL_id  ) VALUES ( "{}",  "{}"); """.format(
            urlFrom, urlTo)
        # print(sqlInsert)
        result = cur.execute(sqlInsert)

        sqlSelect = """SELECT rowid FROM linkbeetwenURL WHERE fk_ToURL_id="{}" ;""".format(urlTo)
        #print(sqlSelect)
        Idlikn = cur.execute(sqlSelect).fetchone()

        linkId = Idlikn[0]

        # Добавление в БД текста ссылки
        #  Разделить сплошной текст на слова
        words = self.separatewords(linkText)


        for i in range(len(words)):
            # Для каждого очередного слова из words
            word = words[i]
            # Если слово не должно быть проиндексировано -> пропуск
            if re.search(r'[A-Za-z0-9]', word) or word in ignorewords:  # \w = [A-Za-z0-9_]
                continue
            else:
                # Проверить находиться ли слово в БД, при необходимости добавить
                wordId = self.getentryid('wordlist', 'word', word)

                # Внести в linkwords информацию
                sqlInsert = """ INSERT INTO linkwords (fk_wordid, fk_linkid) VALUES ('{}','{}'); """.format(wordId, linkId)
                # print(sqlInsert)
                result = cur.execute(sqlInsert)


    # 8. Вспомогательная функция для получения идентификатора и
    # добавления записи, если такой еще нет
    def getentryid(self, table, field, value, createnew=True):
        cur = self.connection.execute(
            "SELECT rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if value == "": return None
        if createnew and res == None:
            cur = self.connection.execute(
                "INSERT INTO %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

# конец класса


# ---------------------------------------------------
def main():
    myCrawler = Crawler("DB_Lab1.db")
    myCrawler.initDB()

    ulrList = list()
    ulrList.append("https://www.kommersant.ru/")
    ulrList.append("https://nsk.rbc.ru/")
    ulrList.append("https://www.vedomosti.ru/")

    start_time = time.time()
    myCrawler.crawl(ulrList, 3)
    print("--- %s seconds ---" % (time.time() - start_time))

    worddf = pd.DataFrame(wordMetrics)
    #pd.to_csv("./worddf.csv")
    plt.plot(worddf)
    plt.show()

    urldf = pd.DataFrame(linkBetweenMetrics)
    plt.plot(urldf)
    plt.show()

    plt.subplot(1,2,1)
    plt.plot(worddf)

    plt.subplot(1,2,2)
    plt.plot(urldf)

    plt.show()

# ---------------------------------------------------
main()
