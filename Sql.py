__author__ = 'waf04'

import MySQLdb


class Sql():
    """
    A class to abstract sql interactions by providing a few convenience methods
    """
    host = str
    user = str
    passwd = str
    db = str
    port = int

    def __init__(self, host, user, passwd, db, port):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port

    def __db(self):
        """
        Creates and returns a db connection

        :return:
        """
        db_conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, port=self.port, db=self.db)
        return db_conn

    def select(self, selectQuery):

        """
        Executes a select query
        :param selectQuery:
        :return:
        """
        #get a db instance and execute the query
        db = self.__db()
        cur = db.cursor()
        cur.execute(selectQuery)

        #parse the results into an array of objects
        columns = cur.description
        result = []
        for value in cur.fetchall():
            tmp = {}
            for (index, column) in enumerate(value):
                tmp[columns[index][0]] = column

            result.append(tmp)

        #close resources
        cur.close()
        db.close()

        return result

    def insert(self, items, table):
        """
        Inserts a list of items into a table
        :param self:
        :param items:
        :param table:
        :return:
        """

        db = self.__db()
        db.autocommit(True)

        cur = db.cursor()
        for item in items:

            #create the query by appending the column names to values
            columns = ''
            values = ''
            for property, value in vars(item).iteritems():
                columns += str(property)+','
                values += '"'+str(value)+'",'

            #remove the last comma
            columns = columns[0:len(columns)-1]
            values = values[0:len(values)-1]

            query = 'INSERT INTO '+table+' ('+columns+') VALUES ('+values+')'

            #insert
            cur.execute(query)

        #close resources
        db.commit()
        cur.close()
        db.close()