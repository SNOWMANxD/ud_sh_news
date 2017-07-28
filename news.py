#!/usr/bin/python3
# -*- coding: utf-8 -*-

import psycopg2
import webpy

SL = "____________________________"
LL = "____________________________________________________________"
centerWidth = 100


def LOGERRORR(cursor):

    # QUERY: LOG ERRORS PER DAY OVER 2.5%
    cursor.execute("""
        SELECT *
        FROM
            (SELECT
                subq.tmstmp,
                round(round((CNERR * 100.0), 2) / round((TOTAL * 100.0), 2), 4)
                AS ERRPERC
            FROM
                (SELECT
                    COUNT(*) AS CNERR,
                    time::timestamp::date AS tmstmp
                FROM log
                WHERE status != '200 OK'
                GROUP BY tmstmp) AS subq,
                (SELECT
                    COUNT(*) AS TOTAL,
                    time::timestamp::date AS tmstmp
                FROM log
                GROUP BY tmstmp) AS subqtotal
            WHERE subq.tmstmp = subqtotal.tmstmp
            GROUP BY
                subq.tmstmp,
                subq.CNERR,
                ERRPERC,
                subqtotal.TOTAL
            ORDER BY
                subq.CNERR DESC
            ) AS subqerr
        WHERE ERRPERC > 0.0025
        ORDER BY
            ERRPERC DESC
        LIMIT 1;""")

    # RETRIEVE (LOGERRORR)
    records = cursor.fetchall()

    # PRINT (LOGERRORR)
    # print(records)
    print(LL.center(centerWidth, " "))
    print(str("###---- DAY WITH HIGHEST LOG ERROR ----###")
          .center(centerWidth, " "))
    date = "Date: " + str(records[0][0])
    errorRate = "Error Rate: " + str(round((records[0][1]*100), 2))+"%"
    print(date.center(centerWidth, " "))
    print(errorRate.center(centerWidth, " "))
    print(LL.center(centerWidth, " "))


def MPAAOAT(cursor):
    # QUERY: MOST POPULAR ARTICLE AUTHORS OF ALL TIME (MPAAOAT)
    cursor.execute("""
        SELECT
            AU_NAME, COUNT(AU_ID) AS CNAID
        FROM
            (SELECT
                a.id, l.id, a.title, a.author AS AU_ID,
                a.slug, au.id, au.name as AU_NAME
            FROM articles AS a
            INNER JOIN log AS l ON CONCAT('/article/',a.slug) = l.path
            INNER JOIN authors AS au ON au.id = a.author) AS subq
        GROUP BY
            AU_ID,
            AU_NAME
        ORDER BY
            CNAID DESC;
        """)

    # RETRIEVE (MPAAOAT)
    records = cursor.fetchall()

    # PRINT (MPAAOAT)
    # print(records)
    print(LL.center(centerWidth, " "))
    print(str("###---- The most popular article authors of all time ----###")
          .center(centerWidth, " "))
    for col in records:
        print(SL.center(centerWidth, " "))
        author = "Author: " + str(col[0])
        views = "Views: " + str(col[1])
        print(author.center(centerWidth, " "))
        print(views.center(centerWidth, " "))
        print(SL.center(centerWidth, " "))
    print(LL.center(centerWidth, " "))


def MPTAOAT(cursor):
    # QUERY: MOST THREE POPULAR ARTICLES OF ALL TIME (MTPAOAT)
    cursor.execute("""
        SELECT
            subq.title,
            COUNT(AID) AS CNAID
        FROM
            (SELECT
                a.id AS AID, l.id AS LID, a.title AS TITLE,
                a.author, a.slug
            FROM articles AS a
            RIGHT JOIN log AS l ON CONCAT('/article/',a.slug) = l.path)
            AS subq
        GROUP BY
            AID,
            subq.title
        ORDER BY
            CNAID DESC
        LIMIT 3;
        """)

    # RETRIEVE (MTPAOAT)
    records = cursor.fetchall()

    # PRINT (MTPAOAT)
    # print(records)
    print(LL.center(centerWidth, " "))
    print(str("###---- The most popular three articles of all time ----###")
          .center(centerWidth, " "))
    for col in records:
        print(SL.center(centerWidth, " "))
        title = "Title: " + str(col[0])
        views = "Views: " + str(col[1])
        print(title.center(centerWidth, " "))
        print(views.center(centerWidth, " "))
        print(SL.center(centerWidth, " "))
    print(LL.center(centerWidth, " "))


def main():
    # Define our connection string
    conn_string = """
                    host='localhost'
                    dbname='news'
                    user='postgres'
                    password='pwasd123test'
                    """

    """ get a connection, if a connect cannot be made,
        an exception will be raised here"""
    conn = psycopg2.connect(conn_string)

    """ conn.cursor will return a cursor object,
        you can use this cursor to perform queries"""
    cursor = conn.cursor()
    # print("Connected!\n")

    print(LL.center(centerWidth, " "))
    print(LL.center(centerWidth, " "))
    print(str("REPORT").center(centerWidth, " "))
    print(LL.center(centerWidth, " "))
    MPTAOAT(cursor)
    MPAAOAT(cursor)
    LOGERRORR(cursor)

if __name__ == "__main__":
    main()
