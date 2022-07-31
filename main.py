#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import sqlite3 as sq
import argparse

#
# block collections
from tools import Logger

cycle = 0
max_year = 0
min_year = 3000
users_commit_view = {}
users_by_years = {}
users_by_month = {}
data_to_sql = []
# end block
#

#
# path and files
parser = argparse.ArgumentParser(description='file path!')
parser.add_argument("--path", default='')
args = parser.parse_args()
path = args.path
filename = ''.join([path, "test_data_commits.log"])


# end block
#


def save_data(to_save_dict, data_time, data_revno):
    if user not in to_save_dict:
        users_commit_view[user] = [(user, data_time, data_revno)]
    else:
        users_commit_view[user].append((user, data_time, data_revno))


def mane_func(file):
    all_commits = 1
    global user, min_year, max_year, cycle
    with open(file, 'r') as file:
        user = ''
        revno = 0
        time = 0
        while True:

            try:
                data = file.next()
            except StopIteration:
                save_data(users_commit_view, time, revno)
                break
            if 'revno:' in data:
                revno = data[7:-1]
            if 'committer:' in data:
                user = data[11:-1]

            if 'timestamp:' in data:
                try:
                    time = datetime.datetime.strptime(data[11:-7],
                                                      "%a %Y-%m-%d %H:%M:%S")  # 2.7 python doesnt have %z (timezone)
                    current_year = data[15:19]
                    if int(current_year) < min_year:
                        min_year = int(current_year)
                    if int(current_year) > max_year:
                        max_year = int(current_year)
                except ValueError:
                    time = ''
            if 'author' in data:
                time = None

            cycle += 1

            if cycle > 3:
                cycle = 0
                all_commits += 1
                save_data(users_commit_view, time, revno)
    all_commits_to_percent = 100 / float(all_commits)
    for item in users_commit_view.values():
        data_to_sql.extend(item)
    with sq.connect('commiters.db') as connection:
        cursor = connection.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS user (
    commiter TEXT,
    time datetime,
    revno INTEGER)""")
        users_exists = cursor.execute("""SELECT count(*) FROM user""")
        if not users_exists.fetchall()[0][0]:
            cursor.executemany('INSERT INTO user VALUES (?, ?, ?)', data_to_sql)

        for year in range(min_year, max_year + 1):
            users_by_year = cursor.execute(
                """SELECT commiter, count(commiter) FROM user WHERE substr(time, 1, 4) = '{}' GROUP BY commiter""".format(
                    year))
            users_by_years[year] = users_by_year.fetchall()
        last_users_commit = cursor.execute("""SELECT commiter, max(time) FROM user GROUP BY commiter""").fetchall()
        for user in range(len(last_users_commit)):
            date_90_days_before = datetime.datetime.strptime(last_users_commit[user][1],
                                                             "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=90)
            item = cursor.execute(
                """SELECT count(commiter), substr(time, 1, 7) as month FROM user WHERE commiter = '{}' 
                AND (time BETWEEN '{}' AND '{}') GROUP BY month""".format(
                    last_users_commit[user][0], date_90_days_before, last_users_commit[user][1]))
            resolt = item.fetchall()
            all_commits_by_month_percent = 100 / float(sum(list(map(lambda item: item[0], resolt))))
            users_by_month[last_users_commit[user][0]] = (resolt, all_commits_by_month_percent)
        my_console = Logger('text.txt')
        print "За все время:\n"

        for key in users_commit_view:
            print "Пользователь:", key, " || коммитов:", len(
                users_commit_view[key]), " || процент от общего числа коммитов:", round(
                len(users_commit_view[key]) * all_commits_to_percent, 2), "%"

        print "\n", "=" * 110, "\n"
        print "По годам:\n"

        for key in users_by_years:
            all_commits_year = sum(list(map(lambda item: item[1], users_by_years[key])))
            all_commits_year_percent = 100 / float(all_commits_year)
            print key, 'Общие чисто коммитов:', all_commits_year
            for user in users_by_years[key]:
                print "\t\t", user[0], '-', user[1], ' || Процент от общего числа:', round(
                    int(user[1]) * all_commits_year_percent, 2), "%"

        print "\n", "=" * 110, "\n"
        print "За 90 дней с последнего коммита:\n"

        for key in users_by_month:
            print 'Пользователь:', key
            for month in users_by_month[key][0]:
                percent = round(month[0] * users_by_month[key][1], 2)
                print '\t\t', 'год-месяц:', month[1], ' || коммитов:', month[0], \
                    ' || % от коммитов за последние 90 дней:', percent, '%'
        my_console.close()


mane_func(filename)
