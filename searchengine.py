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


# ------------------------------------------
def main():
    """ основная функция main() """
    mySeacher = Seacher("DB_Lab1.db")
    mySeacher.getWordsIds('частичная мобилизация')


# ------------------------------------------
main()
