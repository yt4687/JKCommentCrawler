#!/usr/bin/python3

import argparse
import configparser
import datetime
import dateutil.parser
import json
import lxml.etree as ET
import os
from pprint import pprint
import shutil
import sys
import time
import traceback
import xml.dom.minidom as minidom
import requests
import MySQLdb
from lxml import etree
from MySQLdb.cursors import DictCursor

import JKComment

# バージョン情報
__version__ = '1.0.0'

def main():

    # 引数解析
    parser = argparse.ArgumentParser(description = 'ニコ生に移行した新ニコニコ実況の過去ログを取得し、Nekopanda 氏が公開されている旧ニコニコ実況の過去ログデータ一式と互換性のあるファイル・フォルダ構造で保存するツール', formatter_class = argparse.RawTextHelpFormatter)
    parser.add_argument('Channel', help = '取得する実況チャンネル (ex: jk211)  all を指定すると全チャンネル取得する')
    parser.add_argument('Date', help = '取得する日付 (ex: 2020-12-19)')
    parser.add_argument('-v', '--version', action='version', help = 'バージョン情報を表示する', version='JKCommentCrawler version ' + __version__)
    args = parser.parse_args()

    # 引数
    jikkyo_id = args.Channel.rstrip()
    date = dateutil.parser.parse(args.Date.rstrip())

    # 設定読み込み
    config_ini = os.path.dirname(os.path.abspath(sys.argv[0])) + '/JKCommentCrawler.ini'
    if not os.path.exists(config_ini):
        raise Exception('JKCommentCrawler.ini が存在しません。JKCommentCrawler.example.ini からコピーし、\n適宜設定を変更して JKCommentCrawler と同じ場所に配置してください。')
    config = configparser.ConfigParser()
    config.read(config_ini, encoding='UTF-8')

    jkcomment_folder = config.get('Default', 'jkcomment_folder').rstrip('/')
    nicologin_mail = config.get('Default', 'nicologin_mail')
    nicologin_password = config.get('Default', 'nicologin_password')
    discord_webhookurl = config.get('Default', 'webhook_url')
    sql_username = config.get('SQLSetting', 'SQL_USername')
    sql_password = config.get('SQLSetting', 'SQL_password')
    sql_dbname = config.get('SQLSetting', 'SQL_dbname')
    sql_host = config.get('SQLSetting', 'SQL_host')
    sql_port = config.get('SQLSetting', 'SQL_port')

    # 行区切り
    print('=' * shutil.get_terminal_size().columns)


    def get(jikkyo_id, date):

        # インスタンスを作成
        jkcomment = JKComment.JKComment(jikkyo_id, date, nicologin_mail, nicologin_password)
        print(f"{date.strftime('%Y/%m/%d')} 中に放送された {JKComment.JKComment.getJikkyoChannelName(jikkyo_id)} のコメントを取得します。")

        message = (f"{date.strftime('%Y/%m/%d')} 中に放送された {JKComment.JKComment.getJikkyoChannelName(jikkyo_id)} のコメントを取得します。\n```")

        def send_discord(message):
            headers = {'Content-Type': 'application/json'}
            # メッセージ
            payload = {'content': message}
            response = requests.post(discord_webhookurl, json.dumps(payload), headers = headers)

            return response

        # リトライ回数
        retry_maxcount = 3
        retry_count = 1
        while (retry_count <= retry_maxcount):

            # コメントデータ（XML）を取得
            try:
                comment_xmlobject = jkcomment.getComment(objformat='xml')
                break  # ループを抜ける
            # 処理中断、次のチャンネルに進む
            except JKComment.LiveIDError as ex:
                print(f"{date.strftime('%Y/%m/%d')} 中に放送された番組が見つかりませんでした。")
                print('=' * shutil.get_terminal_size().columns)
                message += (f"{date.strftime('%Y/%m/%d')} 中に放送された番組が見つかりませんでした。")+"```"
                responsedi = send_discord(message)
        	    
                return  # この関数を抜ける
            # 捕捉された例外
            except (JKComment.SessionError, JKComment.ResponseError, JKComment.WebSocketError) as ex:
                print('/' * shutil.get_terminal_size().columns, file=sys.stderr)
                print(f"エラー発生時刻: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')} 実況ID: {jikkyo_id} リトライ回数: {retry_count}", file=sys.stderr)
                print(f"エラー: [{ex.__class__.__name__}] {ex.args[0]}", file=sys.stderr)
                print('/' * shutil.get_terminal_size().columns, file=sys.stderr)
                message += ('/' * shutil.get_terminal_size().columns)+"\n"
                message += (f"エラー発生時刻: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')} 実況ID: {jikkyo_id} リトライ回数: {retry_count}\n")
                message += (f"エラー: [{ex.__class__.__name__}] {ex.args[0]}\n")
                message += ('/' * shutil.get_terminal_size().columns)+"\n"
            # 捕捉されない例外
            except Exception as ex:
                print('/' * shutil.get_terminal_size().columns, file=sys.stderr)
                print(f"エラー発生時刻: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')} 実況ID: {jikkyo_id} リトライ回数: {retry_count}", file=sys.stderr)
                print(f"エラー: [{ex.__class__.__name__}] {ex.args[0]}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                print('/' * shutil.get_terminal_size().columns, file=sys.stderr)
                message += ('/' * shutil.get_terminal_size().columns)+"\n"
                message += (f"エラー発生時刻: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')} 実況ID: {jikkyo_id} リトライ回数: {retry_count}\n")
                message += (f"エラー: [{ex.__class__.__name__}] {ex.args[0]}\n")
                message += traceback.print_exc+"\n"
                message += ('/' * shutil.get_terminal_size().columns)+"\n"

            # リトライカウント
            retry_count = retry_count + 1

            # 3 秒スリープ
            if retry_count <= retry_maxcount:
                time.sleep(3)

        # 3 回リトライしてもうまくいかなかったら終了
        if retry_count >= retry_maxcount:
            print('リトライに失敗しました。スキップします。')
            print('=' * shutil.get_terminal_size().columns)
            message += ('リトライに失敗しました。スキップします。\n```@everyone')
            responsedi = send_discord(message)
            print(responsedi)
            return

        # XML をフォーマットする
        # lxml.etree を使うことで属性の順序を保持できる
        # 参考: https://banatech.net/blog/view/19
        def format_xml(elem):
            # xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml = ET.tostring(elem, encoding='UTF-8', pretty_print=True).decode('UTF-8').replace('>\n  ', '>\n')  # インデントを除去
            xml = xml.replace('<packet>\n', '').replace('</packet>', '').replace('<packet/>', '')
            return xml.rstrip()

        # SQLに書き込みをする
        def writesql(jikkyoid, sourcedata):
            # ファイル書き込みの方はXMLの親要素まで消すので元データを自分で加工する
            sourcedata = ET.tostring(sourcedata, encoding='UTF-8', pretty_print=True).decode('UTF-8').replace('>\n  ', '>\n')  # インデントを除去
            root = etree.fromstring(sourcedata)
        
            # 接続するDBを定義
            # シングルクォーテーションが邪魔するので消す(いい方法があったら直すかも、一回しか通さないからいいんじゃないかとも思う)
            thost = sql_host.replace("'", "")
            tuser = sql_username.replace("'", "")
            tpasswd = sql_password.replace("'", "")
            tdb = sql_dbname.replace("'", "")
            connection = MySQLdb.connect(host = thost, user = tuser, passwd = tpasswd, db = tdb, port = int(sql_port), charset = 'utf8mb4')
            cursor = connection.cursor(DictCursor)

            # テーブルがない場合は作成する
            cursor.execute('create table if not exists '+jikkyoid+' (thread varchar(50) not null, no int(6) not null, vpos int(20) not null, date int(20) not null, date_usec int(15) not null, mail varchar(50) not null, user_id varchar(100) not null, premium varchar(1) not null, anonymity varchar(1) not null, text varchar(10000) not null, primary key(thread, no));')
            
            print('SQLデータベースへの書き込みを行っています...')
            #実際の書き込み処理
            for elem in root.iter('chat'):

                # 登録があるかを確認
                cursor.execute('SELECT * FROM ' + jikkyoid + ' WHERE thread=%s and no=%s;', (elem.get('thread'), elem.get('no')))

                # 新規に登録する場合
                if cursor.rowcount == 0:
                    
                    # 配列をセット
                    params = []
                    try:
                        # vposが空のことがある
                        if elem.get('vpos') == None:
                            sqlvpos = 0
                        else:
                            sqlvpos = elem.get('vpos')
                        # date_usecが空のことがある
                        if elem.get('date_usec') == None:
                            sqldate_usec = '0'
                        else:
                            sqldate_usec = elem.get('date_usec')
                        # mailが空のことがある
                        if elem.get('mail') == None:
                            sqlmail = '0'
                        else:
                            sqlmail = elem.get('mail')
                        # premium属性の処理
                        if elem.get('premium') == "1":
                            sqlpremium = "1"
                        else:
                            sqlpremium = "0"
                        # anonymity属性の処理
                        if elem.get('anonymity') == "1":
                            sqlanonymity = "1"
                        else:
                            sqlanonymity = "0"

                        # sql文を生成
                        sql_insert = "INSERT INTO " + jikkyoid + " (thread, no, vpos, date, date_usec, mail, user_id, premium, anonymity, text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                        sql_data = (elem.get('thread'), elem.get('no'), sqlvpos, elem.get('date'), sqldate_usec, sqlmail, elem.get('user_id'), sqlpremium, sqlanonymity, elem.text)
                        
                        # 配列に書き込み
                        params.append(sql_data)
                        
                        #print(sql_insert, sql_data)
                        # sqlを実行
                        #cursor.execute(sql_insert, sql_data)
                        # コミットする
                        #connection.commit()

                    except MySQLdb.Error as e:
                        print('MySQLdb.Error: ', e)
                        print('書き込めなかった項目 スレッド番号：' + elem.get('thread') + ' 連番：' + elem.get('no'))

                    # 生成したデータをまとめて書き込み
                    sql_insert = "INSERT INTO " + jikkyoid + " (thread, no, vpos, date, date_usec, mail, user_id, premium, anonymity, text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    cursor.executemany(sql_insert,params)
                    # コミットする
                    connection.commit()
                    #print(params)

                # 既にデータがある場合    
                elif cursor.rowcount == 1:
                    #print('同一のデータが登録されているためスキップします スレッド番号：' + elem.get('thread') + ' 連番：' + elem.get('no'))
                    pass

                # それ以外、1件以上のデータがある場合
                #else :
                    #print('データに異常があります')
                    #break
            # セッションを閉じる
            connection.close()

        # XML にフォーマット
        comment_xml = format_xml(comment_xmlobject)
        # SQLに書き込み
        writesql(jikkyo_id, comment_xmlobject)

        # ファイル名・フォルダ
        os.makedirs(f"{jkcomment_folder}/{jikkyo_id}/{date.strftime('%Y')}/", exist_ok=True)
        filename = f"{jkcomment_folder}/{jikkyo_id}/{date.strftime('%Y')}/{date.strftime('%Y%m%d')}.nicojk"

        # 既にファイルが存在していたら文字数を取得
        if os.path.exists(filename):
            with open(filename, 'r', encoding='UTF-8') as f:
                filelength = len(f.read())
        else:
            filelength = 0

        # コメントデータ（XML）を保存
        if comment_xml == '':
            print(f"{date.strftime('%Y/%m/%d')} 中のコメントが 0 件のため、ログの保存をスキップします。")
            message += (f"{date.strftime('%Y/%m/%d')} 中のコメントが 0 件のため、ログの保存をスキップします。\n```")
        # 以前取得したログの方が今取得したログよりも文字数が多いとき
        # タイムシフトの公開期限が終了したなどの理由で以前よりもログ取得が少なくなる場合に上書きしないようにする
        elif filelength > len(comment_xml):
            print('以前取得したログの方が文字数が多いため、ログの保存をスキップします。')
            message += ('以前取得したログの方が文字数が多いため、ログの保存をスキップします。\n```')
        else:
            with open(filename, 'w', encoding='UTF-8') as f:
                f.write(comment_xml)
                print(f"ログを {filename} に保存しました。")
                message += (f"ログを {filename} に保存しました。\n```")

        responsedi = send_discord(message)
        print(responsedi)

        # 行区切り
        print('=' * shutil.get_terminal_size().columns)


    # コメントデータ（XML）を全てのチャンネル分取得
    if jikkyo_id.lower() == 'all':
        for jikkyo_id_ in JKComment.JKComment.getJikkyoIDList():
            get(jikkyo_id_, date)

    # コメントデータ（XML）を単一チャンネル分取得
    else:
        get(jikkyo_id, date)


if __name__ == '__main__':
    main()
