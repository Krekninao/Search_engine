import sqlite3


class Seacher:

    def dbcommit(self):
        """ Зафиксировать изменения в БД """
        self.con.commit()

    def __init__(self, dbFileName):
        """  0. Конструктор """
        # открыть "соединение" получить объект "сonnection" для работы с БД
        self.con = sqlite3.connect(dbFileName)

    def __del__(self):
        """ 0. Деструктор  """
        # закрыть соединение с БД
        self.dbcommit()
        self.con.close()

    def getWordsIds(self, queryString):
        """
        Получение идентификаторов для каждого слова в queryString
        :param queryString: поисковый запрос пользователя
        :return: список wordlist.rowid искомых слов
        """

        # Привести поисковый запрос к верхнему регистру
        queryString = queryString.lower()

        # Разделить на отдельные искомые слова
        queryWordsList = queryString.split()

        # список для хранения результата
        rowidList = list()

        # Для каждого искомого слова
        for word in queryWordsList:
            # Сформировать sql-запрос для получения rowid слова, указано ограничение на кол-во возвращаемых результатов (LIMIT 1)
            sql = "SELECT rowid FROM wordlist WHERE word =\"{}\" LIMIT 1; ".format(word)

            # Выполнить sql-запрос. В качестве результата ожидаем строки содержащие целочисленный идентификатор rowid
            result_row = self.con.execute(sql).fetchone()

            # Если слово было найдено и rowid получен
            if result_row != None:
                # Искомое rowid является элементом строки ответа от БД (особенность получаемого результата)
                word_rowid = result_row[0]

                # поместить rowid в список результата
                rowidList.append(word_rowid)
                print("  ", word, word_rowid)
            else:
                # в случае, если слово не найдено приостановить работу (генерация исключения)
                raise Exception("Одно из слов поискового запроса не найдено:" + word)

        # вернуть список идентификаторов
        return rowidList

    def getMatchRows(self, queryString):
        """
        Поиск комбинаций из всезх искомых слов в проиндексированных url-адресах
        :param queryString: поисковый запрос пользователя
        :return: 1) список вхождений формата (urlId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."
        """

        # Разбить поисковый запрос на слова по пробелам
        queryString = queryString.lower()
        wordsList = queryString.split(' ')

        # получить идентификаторы искомых слов
        wordsidList = self.getWordsIds(queryString)

        # Созать переменную для полного SQL-запроса
        sqlFullQuery = """"""

        # Созать объекты-списки для дополнений SQL-запроса
        sqlpart_Name = list()  # имена столбцов
        sqlpart_Join = list()  # INNER JOIN
        sqlpart_Condition = list()  # условия WHERE

        # Конструктор SQL-запроса (заполнение обязательной и дополнительных частей)
        # обход в цикле каждого искомого слова и добавлене в SQL-запрос соответствующих частей
        for wordIndex in range(0, len(wordsList)):

            # Получить идентификатор слова
            wordID = wordsidList[wordIndex]

            if wordIndex == 0:
                # обязательная часть для первого слова
                sqlpart_Name.append("""w0.fk_URLid    urlid  --идентификатор url-адреса""")
                sqlpart_Name.append("""   , w0.location w0_loc --положение первого искомого слова""")

                sqlpart_Condition.append("""WHERE w0.fk_wordid={}     -- совпадение w0 с первым словом """.format(
                    wordID))

            else:
                # Дополнительная часть для 2,3,.. искомых слов

                if len(wordsList) >= 2:
                    # Проверка, если текущее слово - второе и более
                    # Добавить в имена столбцов
                    sqlpart_Name.append(
                        """ , w{}.location w{}_loc --положение следующего искомого слова""".format(wordIndex,
                                                                                                   wordIndex))

                    # Добавить в sql INNER JOIN
                    sqlpart_Join.append("""INNER JOIN wordlocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                                on w0.fk_URLid=w{}.fk_URLid -- условие объединения""".format(wordIndex, wordIndex, wordIndex))
                    # Добавить в sql ограничивающее условие
                    sqlpart_Condition.append(
                        """  AND w{}.fk_wordid={} -- совпадение w{}... с cоответсвующим словом """.format(wordIndex,
                                                                                                       wordID,
                                                                                                       wordIndex))
                    pass
            pass

            # Объеднение запроса из отдельных частей

        # Команда SELECT
        sqlFullQuery += "SELECT "

        # Все имена столбцов для вывода
        for sqlpart in sqlpart_Name:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # обязательная часть таблица-источник
        sqlFullQuery += "\n"
        sqlFullQuery += "FROM wordlocation w0 "

        # часть для объединения таблицы INNER JOIN
        for sqlpart in sqlpart_Join:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # обязательная часть и дополнения для блока WHERE
        for sqlpart in sqlpart_Condition:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # Выполнить SQL-запроса и извлеч ответ от БД
        print(sqlFullQuery)
        cur = self.con.execute(sqlFullQuery)
        rows = [row for row in cur]

        return rows, wordsidList

    def normalizeScores(self, scores, smallIsBetter=0):

        resultDict = dict()  # словарь с результатом

        vsmall = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
        minscore = min(scores.values())  # получить минимум
        maxscore = max(scores.values())  # получить максимум

        # перебор каждой пары ключ значение
        for (key, val) in scores.items():

            if smallIsBetter:
                # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
                # ранг нормализованный = мин. / (тек.значение  или малую величину)
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                # вычисление ранга как доли от макс.
                # ранг нормализованный = тек. значения / макс.
                resultDict[key] = float(val) / maxscore

        return resultDict

    def distancescore(self, rows):
        # Если есть только одно слово, любой документ выигрывает!
        if len(rows[0]) <= 2: return dict([(row[0], 1.0) for row in rows])

        # Инициализировать словарь большими значениями
        mindistance = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            dist = sum([abs(row[i] - row[i - 1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]: mindistance[row[0]] = dist
        return self.normalizeScores(mindistance, smallIsBetter=1)

# ------------------------------------------
def main():
    """ основная функция main() """
    mySeacher = Seacher("DB_Lab1.db")
    rows, wordsidList = mySeacher.getMatchRows('частичная мобилизация')
    print (rows)
    print(wordsidList)
    diction = mySeacher.distancescore(rows)
    print(diction)


# ------------------------------------------
main()
