import json
import threading
import time
from email.message import EmailMessage
from io import BytesIO
import sqlite3
import datetime
from sqlite3 import IntegrityError
import smtplib, ssl
import mimetypes

import pytz
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Poll
from telegram.ext import Updater, CommandHandler, CallbackContext, CallbackQueryHandler

LOGIN_URL = "https://api.glassen-it.com/component/socparser/authorization/login"
THREADS_URL = "https://api.glassen-it.com/component/socparser/users/getuserthreads"
REFERENCES_URL = "https://api.glassen-it.com/component/socparser/users/getreferences"
URL = "https://api.glassen-it.com/component/socparser/content/getReportDocxRef?period=%s&thread_id=%s"
USER_INFO = "https://api.glassen-it.com/component/socparser/users/getuser"

SESSION = requests.session()

period = {"1": "дневной", "2": "недельный", "3": "месячный"}
period_s = {"1": "day", "2": "week", "3": "month"}

type_report = {
    "1": "дневной отчет отправлен (report.doc)",
    "2": "недельный отчет отправлен (report.doc)",
    "3": "месячный отчет отправлен (report.doc)"
}

EMAIL = 'sityracoon07@gmail.com'
EMAIL_PASSWORD = "qweewq123q"


def login(session):
    payload = {
        "login": "java_api",
        "password": "4yEcwVnjEH7D"
    }
    response = session.post(LOGIN_URL, json=payload)
    if not response.ok:
        raise Exception("can not login")
    return session


def send_message_email(email_to, file, file_name, report_text):
    port = 465
    context = ssl.create_default_context()

    msg = EmailMessage()
    msg['Subject'] = 'Автоматическая рассылка отчёта по выбранным субъектам/событиям'
    msg['From'] = EMAIL
    msg['To'] = email_to

    file.seek(0)
    binary_data = file.read()
    maintype, _, subtype = (mimetypes.guess_type(file_name)[0] or 'application/octet-stream').partition("/")
    msg.set_content('Добрый день! \n'
                    'В соответствии с выбранными вами временным интервалом и объектами мониторинга был'
                    f' сформирован отчёт по запросу по следующим субъектам/событиям: \n{report_text}'
                    )
    msg.add_attachment(binary_data, maintype=maintype, subtype=subtype, filename=file_name)

    with smtplib.SMTP_SSL("smtp.glassen-it.com", port, context=context) as server:
        server.login(EMAIL_LOGIN, EMAIL_PASSWORD)
        server.send_message(msg)

# #
# SESSION = login(requests.session())
# uri = "https://api.glassen-it.com/component/socparser/content/getReportDocxRef?period=day&thread_id=4205"
# report = SESSION.get(uri)
# i = BytesIO(report.content)
# file_name = bytes(
#         report.headers.get('Content-Disposition').replace("attachment;filename=", "").replace(
#             '"', ""), 'latin1').decode('utf-8')
# send_message_email('gusevoleg96@gmail.com', BytesIO(report.content), file_name, "")


def get_email(user_id):
    return SESSION.post(USER_INFO, json={
        "uid": user_id
    }).json().get("email")


def get_threads_by_id(user_id):
    response = SESSION.post(THREADS_URL, json={
        "user_id": user_id,
        "extended": True
    })
    return response.json()[0]


def get_items_by_id(user_id, all=False):
    response_json = SESSION.post(REFERENCES_URL, json={
        "group_id": user_id,
        "is_user_id": True
    }).json()
    # + response_json[3].get("items", [])
    if all:
        return response_json
    return response_json[1].get("items", [])


def get_objects(user_id, query_data):
    items_id = get_items_by_id(user_id, True)
    menu_main = []

    i = 1
    sob = items_id[1].get("items", [])
    su = items_id[3].get("items", [])
    k = len(sob) + 2
    first_button_row = sob
    flast_button_row = su
    for item in range(0, max(len(sob), len(su))):
        try:
            menu_main.append([InlineKeyboardButton(first_button_row[item].get("keyword").replace('"', ""),
                                                   callback_data=f"{query_data}_{i}_t"),
                              InlineKeyboardButton(flast_button_row[item].get("keyword").replace('"', ""),
                                                   callback_data=f'{query_data}_{k}_t')])
        except Exception:
            try:
                menu_main.append([InlineKeyboardButton(first_button_row[item].get("keyword").replace('"', ""),
                                                       callback_data=f'{query_data}_{i}_t'),
                                  InlineKeyboardButton(" ",
                                                       callback_data=f'{query_data}_{k}_t')]
                                 )
            except Exception:
                menu_main.append([InlineKeyboardButton(" ",
                                                       callback_data=f'{query_data}_{i}_t'),
                                  InlineKeyboardButton(flast_button_row[item].get("keyword").replace('"', ""),
                                                       callback_data=f'{query_data}_{k}_t')])
        i += 1
        k += 1


    menu_main.append([InlineKeyboardButton('Ok', callback_data=f'{query_data}_stop')])

    reply_markup = InlineKeyboardMarkup(menu_main)
    return reply_markup


def start(update: Update, context: CallbackContext) -> None:
    try:
        user = update.message.text.replace("/start", "").strip()
    except Exception:
        user = None
    this_chat_id = update.message.chat_id
    user_id_db = get_user_id(this_chat_id)
    email = get_email(user)
    if user_id_db is None:
        if user:
            db_save(str(user), str(this_chat_id), str(email))
            update.message.reply_text(f'Добрый день! \nЯ буду помогать Вам с отчетами. \n /create_report')
        else:
            update.message.reply_text(f'Не могу Вас найти, пожалуйста, обратитесь к администраторам')
    else:
        if user and user_id_db != user:
            db_save(str(user), str(this_chat_id), str(email))
        update.message.reply_text(f'Добрый день! \nЯ буду помогать Вам с отчетами. \n /create_report')


def chek_user_db(update):
    if not get_user_id(str(update.message.from_user.id)):
        update.message.reply_text(
            f'Не могу Вас найти. Возможно вы удалили свой аккаунт, войдите еще раз или обратитесь к администраторам')
        return False
    return True


def create_report(update: Update, context: CallbackContext) -> None:

    if chek_user_db(update):
        menu_main = [[InlineKeyboardButton('День', callback_data='1_d')],
                     [InlineKeyboardButton('Неделя', callback_data='2_d')],
                     [InlineKeyboardButton('Месяц', callback_data='3_d')]]

        reply_markup = InlineKeyboardMarkup(menu_main)
        update.message.reply_text('Выберите период отчета:', reply_markup=reply_markup)


def delete(update: Update, context: CallbackContext) -> None:
    if chek_user_db(update):
        chat_id = update.message.chat_id
        db_delete_week(chat_id)
        db_delete_day(chat_id)
        db_delete_month(chat_id)
        db_delete_chat_id(chat_id)
        update.message.reply_text("Вы удалили свой аккаунт")


def delete_report(update: Update, context: CallbackContext) -> None:
    if chek_user_db(update):

        menu_main = []
        chat_id = str(update.message.chat_id)
        if get_day_id(chat_id):
            menu_main.append([InlineKeyboardButton("Дневной отчет", callback_data='1_r')])
        if get_week_id(chat_id):
            menu_main.append([InlineKeyboardButton("Недельный отчет", callback_data='2_r')])
        if get_month_id(chat_id):
            menu_main.append([InlineKeyboardButton("Месячный отчет", callback_data='3_r')])
        if len(menu_main) > 0:
            reply_markup = InlineKeyboardMarkup(menu_main)
            update.message.reply_text('Выберите период отчета:', reply_markup=reply_markup, parse_mode='Markdown')
        else:
            update.message.reply_text('У Вас нет сохраненных отчетов')


def add_hour(m):
    menu_main = []
    for i in range(0, 6):
        row = []
        for j in range(0, 4):
            h = str(i * 4 + j)
            row.append(InlineKeyboardButton(h, callback_data=f'{m}_{h}_h'))
        menu_main.append(row)
    return menu_main


def add_minutes(m):
    menu_main = []
    for i in range(0, 3):
        row = []
        for j in range(0, 4):
            min = str(i * 20 + j * 5)
            res_min = min
            if len(min) == 1:
                res_min = "0" + res_min
            row.append(InlineKeyboardButton(res_min, callback_data=f'{m}_{res_min}_min'))
        menu_main.append(row)
    return menu_main


def menu_actions(update, bot):
    query = update.callback_query

    if query.data[-1] == 'r':
        chat_id = str(query.from_user.id)

        if query.data[0] == "1":
            db_delete_day(chat_id)
        elif query.data[0] == "2":
            db_delete_week(chat_id)
        else:
            db_delete_month(chat_id)

        query.message.reply_text('Отчет удален', parse_mode='Markdown')
    if query.data[-1] == 'd':
        menu_main = [[InlineKeyboardButton('Разовый отчет',
                                           callback_data=f's_{query.data}_n')],
                     [InlineKeyboardButton('Отчет по расписанию', callback_data=f'{query.data}_reg')],
                     ]
        reply_markup = InlineKeyboardMarkup(menu_main)
        query.edit_message_text('Выберите тип отчета:', reply_markup=reply_markup)

    if query.data == '1_d_reg':
        if get_day_id(str(query.from_user.id)):
            query.edit_message_text('За этот период отчет уже существует')
        else:
            reply_markup = InlineKeyboardMarkup(add_hour(query.data))
            query.edit_message_text('Выберите время отправки (час):', reply_markup=reply_markup)

    elif query.data == '2_d_reg':
        if get_week_id(str(query.from_user.id)):
            query.edit_message_text('За этот период отчет уже существует')
        else:
            reply_markup = InlineKeyboardMarkup(add_hour(query.data))
            query.edit_message_text('Выберите время отправки (час):', reply_markup=reply_markup)
    elif query.data == '3_d_reg':
        if get_month_id(str(query.from_user.id)):
            query.edit_message_text('За этот период отчет уже существует')
        else:
            reply_markup = InlineKeyboardMarkup(add_hour(query.data))
            query.edit_message_text('Выберите время отправки (час):', reply_markup=reply_markup)
    elif query.data[-1] == 'h':
        reply_markup = InlineKeyboardMarkup(add_minutes(query.data))
        query.edit_message_text('Выберите время отправки (минуту):', reply_markup=reply_markup)
    elif query.data[-1] == 'n':
        user_id = get_user_id(str(query.from_user.id))
        reply_markup = get_objects(user_id, query.data)
        query.edit_message_text('Выберите объекты:', reply_markup=reply_markup,)
    elif query.data[-1] == 'p':

        # json_data = json.loads(str(update.effective_message.reply_markup).replace('"', "").replace("\'", '"'))
        check_ = False
        teams = []
        for d in update.effective_message.reply_markup["inline_keyboard"]:

            ok = u'\u2705'
            if ok in d[0]["text"].replace('"', ""):
                check_ = True
                teams.append(d[0]["text"].replace(ok, ""))
            elif len(d) > 1 and ok in d[1]["text"].replace('"', ""):
                check_ = True
                teams.append(d[1]["text"].replace(ok, ""))
        if check_:
            repost_teams = "\n".join(teams)
            if query.data[0] != 's':
                data_split = query.data.split("_")
                title = period.get(data_split[0])
                time = f"{data_split[3]}:{data_split[5]}"
                chat_id = str(query.from_user.id)
                user_id = get_user_id(str(query.from_user.id))
                items_id = get_items_by_id(user_id, True)
                references = ""
                for team in teams:
                    for item_id in items_id[1]["items"]:
                        if team == item_id.get("keyword").replace('"', ""):
                            references += f"&reference_ids[]={item_id.get('id')}"
                            break
                    for item_id in items_id[3]["items"]:
                        if team == item_id.get("keyword").replace('"', ""):
                            references += f"&reference_ids[]={item_id.get('id')}"
                            break
                thread = get_threads_by_id(int(user_id))
                try:
                    report_text = f"Период: {title} \nТемы: {repost_teams}"

                    if data_split[0] == "1":
                        db_save_day(chat_id, thread, references, time, report_text)
                    elif data_split[0] == "2":
                        db_save_week(chat_id, thread, references, time, report_text)
                    else:
                        db_save_month(chat_id, thread, references, time, report_text)
                    now = get_time_now()
                    time_datetime = datetime.datetime(now.year, now.month, now.day, int(data_split[3]), int(data_split[5]))
                    if 0 < (time_datetime - get_time_now()).seconds < 300:
                        uri = URL % (period_s.get(data_split[0]), thread) + references
                        threading.Thread(target=send_message_time, args=(uri, time_datetime, chat_id, report_text)).start()
                    text = f"*Отчет сохранен:* \n*Период*: {title} \n*Время*: {time} \n*Темы*: " + "\n".join(teams)
                    query.edit_message_text(text, parse_mode='Markdown')
                except IntegrityError:
                    query.edit_message_text('За этот период отчет уже существует')
            else:
                query.edit_message_text("Отчет формируется", parse_mode='Markdown')
                period_ = query.data.split("_")[1]
                report_text = f"Период: {period.get(period_)} \nТемы: {repost_teams}"

                threading.Thread(target=send_message, args=(
                    period_, teams, query.bot, query.message.chat_id, get_user_id(str(query.from_user.id)), report_text)).start()
        else:
            reply_markup = InlineKeyboardMarkup(update.effective_message.reply_markup["inline_keyboard"])
            query.edit_message_text(
                text='Вы не выбрали объекты. \nПожалуйста, выберите объекты:',
                reply_markup=reply_markup)

    elif "t" in query.data:
        menu_main = []
        # json_data = json.loads(str(update.effective_message.reply_markup).replace('"', "").replace("\'", '"'))
        i = 1
        for d in update.effective_message.reply_markup["inline_keyboard"]:
            ok = u'\u2705'
            if query.data in d[0]['callback_data']:
                if ok in d[0]["text"]:
                    menu_main.append(
                        [InlineKeyboardButton(d[0]["text"].replace(ok, ""), callback_data=d[0]['callback_data']),
                         InlineKeyboardButton(d[1]["text"], callback_data=d[1]['callback_data']),
                         ])
                else:
                    menu_main.append(
                        [InlineKeyboardButton(f'{ok}{d[0]["text"]}', callback_data=d[0]['callback_data']),
                         InlineKeyboardButton(d[1]["text"], callback_data=d[1]['callback_data']),
                         ])
            elif len(d) > 1 and query.data in d[1]['callback_data']:
                if ok in d[1]["text"]:
                    menu_main.append(
                        [InlineKeyboardButton(d[0]["text"], callback_data=d[0]['callback_data']),
                         InlineKeyboardButton(d[1]["text"].replace(ok, ""), callback_data=d[1]['callback_data']),
                         ])
                else:
                    menu_main.append(
                        [InlineKeyboardButton(d[0]["text"], callback_data=d[0]['callback_data']),
                         InlineKeyboardButton(f'{ok}{d[1]["text"]}', callback_data=d[1]['callback_data']),
                         ])
            else:
                if len(d) > 1:
                    menu_main.append(
                        [InlineKeyboardButton(d[0]["text"], callback_data=d[0]['callback_data']),
                         InlineKeyboardButton(d[1]["text"], callback_data=d[1]['callback_data'])
                         ])
                else:
                    menu_main.append(
                        [InlineKeyboardButton(d[0]["text"], callback_data=d[0]['callback_data']),
                         ])
            i += 1
        reply_markup = InlineKeyboardMarkup(menu_main)
        query.edit_message_text(
            text='Выберите объекты:',
            reply_markup=reply_markup)


def get_report(uri):
    report = SESSION.get(uri)
    i = BytesIO(report.content)
    file_name = bytes(
        report.headers.get('Content-Disposition').replace("attachment;filename=", "").replace(
            '"', ""), 'latin1').decode('utf-8')
    return i, file_name


def send_message(period_, teams, bot, chat_id, user_id, text):
    uri = URL % (period_s.get(period_), get_threads_by_id(user_id))
    items_id = get_items_by_id(user_id, True)
    for team in teams:
        for item_id in items_id[1]["items"]:
            if team == item_id.get("keyword").replace('"', ""):
                uri += f"&reference_ids[]={item_id.get('id')}"
                break
        for item_id in items_id[3]["items"]:
            if team == item_id.get("keyword").replace('"', ""):
                uri += f"&reference_ids[]={item_id.get('id')}"
                break
    i, file_name = get_report(uri)
    # TODO send email

    bot.send_document(chat_id=chat_id,
                      document=i,
                      filename=file_name,
                      caption=text
                      )


def db_save(user_id: str, chat_id: str, email: str):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO users (user_id, chat_id, email) VALUES (?, ?, ?)', (user_id, chat_id, email))
    conn.commit()


def db_delete_chat_id(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM users WHERE chat_id={chat_id}')
    conn.commit()


def get_user_id(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    users = cursor.execute(f"select * from users WHERE `chat_id` = {chat_id}").fetchall()
    if len(users) == 0:
        return None
    return users[0][0]


def get_mail(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    users = cursor.execute(f"select * from users WHERE `chat_id` = {chat_id}").fetchall()
    if len(users) == 0:
        return None
    return users[0][2]


def get_chat_id(user_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    users = cursor.execute(f"select * from users WHERE `user_id` = {user_id}").fetchall()
    if len(users) == 0:
        return None
    return users[-1][1]


def get_day_id(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    day = cursor.execute(f"select * from days WHERE `chat_id` = {chat_id}").fetchall()
    if len(day) == 0:
        return None
    return day[-1]


def db_save_day(chat_id, thread_id, references, time, reporttext):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO days (chat_id, thread_id, refer, time, reporttext) VALUES (?, ?, ?, ?, ?)',
                   (chat_id, thread_id, references, time, reporttext))
    conn.commit()


def db_delete_day(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM days WHERE chat_id={chat_id}')
    conn.commit()


def get_month_id(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    month = cursor.execute(f"select * from months WHERE `chat_id` = {chat_id}").fetchall()
    if len(month) == 0:
        return None
    return month[-1]


def db_save_month(chat_id, thread_id, references, time, reporttext):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO months (chat_id, thread_id, refer, time, reporttext) VALUES (?, ?, ?, ?, ?)',
                   (chat_id, thread_id, references, time, reporttext))
    conn.commit()


def db_delete_month(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM months WHERE chat_id={chat_id}')
    conn.commit()


def get_week_id(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    week = cursor.execute(f"select * from weeks WHERE `chat_id` = {chat_id}").fetchall()
    if len(week) == 0:
        return None
    return week[-1]


def db_delete_week(chat_id):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM weeks WHERE chat_id={chat_id}')
    conn.commit()


def db_save_week(chat_id, thread_id, references, time, reporttext):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO weeks (chat_id, thread_id, refer, time, reporttext) VALUES (?, ?, ?, ?, ?)',
                   (chat_id, thread_id, references, time, reporttext))
    conn.commit()


def check_and_send_message(now, d, period):
    h_r = d[3].split(":")
    time = datetime.datetime(now.year, now.month, now.day, int(h_r[0]), int(h_r[1]))
    if 0 < (time - now).seconds < 300:
        uri = URL % (period, d[1]) + d[2]
        print(uri)
        threading.Thread(target=send_message_time, args=(uri, time, int(d[0]), d[4])).start()


def send_message_time(uri, time_, chat_id, report_text):
    try:
        print(uri)
        print(time_)
        i, file_name = get_report(uri)
        sleep_time = (time_ - get_time_now()).seconds + 5
        if sleep_time > 0:
            time.sleep(sleep_time)
        print("send" + str(uri))
        # TODO send email
        try:
            updater.bot.send_document(chat_id=chat_id,
                                      document=i,
                                      filename=file_name,
                                      caption=report_text
                                      )
        except Exception:
            pass
        try:
            send_message_email(get_mail(chat_id), i, file_name, report_text)
        except Exception as e:
            print(e)
    except Exception as e:
        print(e)


def send_messages_time():
    try:
        print("send_messages_time" + str(get_time_now()))

        now = get_time_now()
        conn = sqlite3.connect('database.db', check_same_thread=False)
        cursor = conn.cursor()
        days = cursor.execute(f"select * from days").fetchall()
        weeks = cursor.execute(f"select * from weeks").fetchall()
        months = cursor.execute(f"select * from months").fetchall()
        for d in days:
            check_and_send_message(now, d, "day")
        for w in weeks:
            check_and_send_message(now, w, "week")
        for m in months:
            check_and_send_message(now, m, "month")
    except Exception as e:
        print("send_messages_time" + str(e))


# schedule.every(5).minutes.do(send_messages_time)


def start_schedule():
    time.sleep(305 - int(get_time_now().timestamp()) % 300)
    print(get_time_now())
    while True:
        try:
            send_messages_time()
            time.sleep(300 - int(get_time_now().timestamp()) % 300)
        except Exception as e:
            print(e)
            time.sleep(1)


def get_time_now():
    # return datetime.datetime.now(pytz.timezone('Etc/GMT-3'))
    return datetime.datetime.now()

    # return datetime.datetime.now() + datetime.timedelta(hours=3)


updater = Updater('2118643730:AAFLO8GyzQQDIFI1nmS2_hPdqgh-De4WGrs')

if __name__ == '__main__':

    print(get_time_now())

    while True:
        SESSION = login(SESSION)
        threading.Thread(target=start_schedule).start()
        send_messages_time()
        updater.dispatcher.add_handler(CommandHandler('start', start))
        updater.dispatcher.add_handler(CommandHandler('create_report', create_report))
        updater.dispatcher.add_handler(CommandHandler('delete_report', delete_report))
        updater.dispatcher.add_handler(CommandHandler('delete', delete))

        updater.dispatcher.add_handler(CallbackQueryHandler(menu_actions))
        updater.start_polling()
        updater.idle()
