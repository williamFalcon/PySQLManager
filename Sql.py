__author__ = 'waf04'

import MySQLdb
import traceback
import json

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

    def executeGenericQuery(self, query):
         #get a db instance and execute the query
        db = self.__db()
        cur = db.cursor()
        db.autocommit(True)

         #insert
        try:
            cur.execute(query)

        except Exception:
            print('insert error')
            traceback.print_exc()


    def select(self, selectQuery):

        """
        Executes a select query
        :rtype : object
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
        db = None

        return result

    def insert(self, items, table):
        """
        Inserts a list of items into a table
        :rtype : None
        :param self:
        :param items:
        :param table:
        """

        db = self.__db()
        db.autocommit(True)

        cur = db.cursor()
        for item in items:

            try:
                #create the query by appending the column names to values
                columns = ''
                values = ''
                for property, value in vars(item).iteritems():
                    columns += str(property)+','
                    #format the text
                    formatedValue = "%s" % (value,)

                    values += '"' + formatedValue + '",'

                #remove the last comma
                columns = columns[0:len(columns)-1]
                values = values[0:len(values)-1]

                query = 'INSERT INTO '+table+' ('+columns+') VALUES ('+values+')'

                #insert
                cur.execute(query)

            except Exception:
                traceback.print_exc()

        #close resources
        db.commit()
        cur.close()
        db.close()
        db = None

    def insertFieldsOnly(self, items, attributes, table):
        """
        Inserts a list of items into a table
        :param attributes:
        :rtype : None
        :param self:
        :param items:
        :param table:
        """

        db = self.__db()
        db.autocommit(True)

        cur = db.cursor()
        for item in items:

            try:
                #create the query by appending the column names to values
                columns = ''
                values = ''
                for property, value in vars(item).iteritems():

                    if property in attributes:
                        columns += str(property)+','
                        formatedValue = "%s" % (value,)
                        values += '"' + formatedValue + '",'

                #remove the last comma
                columns = columns[0:len(columns)-1]
                values = values[0:len(values)-1]

                query = 'INSERT INTO '+table+' ('+columns+') VALUES ('+values+')'

                #insert
                cur.execute(query)

            except Exception:
                traceback.print_exc()

        #close resources
        db.commit()
        cur.close()
        db.close()
        db = None

    def update(self, items, table):
        """
        Updates a list of items into a table
        :rtype : None
        :param self:
        :param items:
        :param table:
        """

        db = self.__db()
        db.autocommit(True)

        cur = db.cursor()
        for item in items:

            #create the query by appending the column names to values
            columns = []
            values = []
            for property, value in vars(item).iteritems():

                #skip myId
                if 'myId' in str(property):
                    continue

                columns.append(str(property))
                values.append(str(value))

            query = 'UPDATE '+table+' SET '

            for i in range(len(columns)):
                column = columns[i]

                formatedValue = "%s" % (values[i],)
                value = '\''+formatedValue+'\''

                query = query + column + '=' + value + ', '

            query = query[0:len(query)-2]

            if hasattr(item, 'myId'):
                query = query + ' WHERE myId = '+str(item.myId)
            
            #escape
            query = query.replace("'s", r"\'")
            
            #insert
            try:
                cur.execute(query)

            except Exception:
                print('insert error\nSQL Query Failed: ' + query)
                traceback.print_exc()

        #close resources
        db.commit()
        cur.close()
        db.close()
        db = None

    def updateFieldsOnly(self, items, table, fields):
        """
        Updates a list of items into a table
        :rtype : None
        :param self:
        :param items:
        :param table:
        """

        db = self.__db()
        db.autocommit(True)

        cur = db.cursor()
        for item in items:

            #create the query by appending the column names to values
            columns = []
            values = []
            for property, value in vars(item).iteritems():

                #skip myId
                if 'myId' in str(property):
                    continue

                if property in fields:
                    columns.append(str(property))
                    values.append(str(value))

            query = 'UPDATE '+table+' SET '

            for i in range(len(columns)):
                column = columns[i]

                formatedValue = "%s" % (values[i],)
                value = '\''+formatedValue+'\''

                query = query + column + '=' + value + ', '

            query = query[0:len(query)-2]

            if hasattr(item, 'myId'):
                query = query + ' WHERE myId = '+str(item.myId)

            #insert
            try:
                cur.execute(query)

            except Exception:
                print('insert error')
                traceback.print_exc()

        #close resources
        db.commit()
        cur.close()
        db.close()
        db = None
