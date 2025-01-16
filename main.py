import pymysql
import os
from config import host, user, password, db_name, ending, shard, input_file_path_1, output_file_path_1, output_file_path_2
import keyboard
import time
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
start = True
file_id = 1
break_flag = False

def read_and_write_file(input_file_path, output_file_path_sp1, output_file_path_sp2):
    try:
        with (open(input_file_path, 'r') as input_file, open(output_file_path_sp1, 'w') as outfile_1,
              open(output_file_path_sp2, 'w') as outfile_2):
            for line in input_file:
                event_type = (line.split(',')[0])    # выбираем нужный параметр ивента
                event_type_num = int(event_type.split(' ')[1]) # убираем название параметра, оставляя само значение
                # первый сплиттер берет записи только с нечетными id
                if event_type_num % 2 != 0:
                    outfile_1.write(line)
                # второй сплиттер берет записи только с четными id
                if event_type_num % 2 == 0:
                    outfile_2.write(line)

        print(f"Content successfully written to {output_file_path_sp1} and {output_file_path_sp2}")

    except FileNotFoundError:
        print(f"Error: The file {input_file_path} does not exist.")

try:
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )
    print("successfully connected...")
    print("#" * 20)
    try:
        print("Нажмите Q, чтобы выйти")
        while True:
            with connection.cursor() as cursor:
                if start is False:
                    # начало работы
                    file_id_select = "SELECT id FROM `pipeline` WHERE filename = (SELECT MIN(filename) FROM `pipeline`)"
                    cursor.execute(file_id_select)
                    file_id_dict = cursor.fetchone()
                    file_id = file_id_dict['id']
                    file_id = file_id + 1

                    filename_select = "SELECT MIN(filename) FROM `pipeline` WHERE stage_splitter = 'new'"
                    start = True
                else:
                    # уже есть обработанные файлы
                    filename_select = "SELECT filename FROM `pipeline` WHERE stage_splitter = 'new' AND id = %s" % file_id

                cursor.execute(filename_select)
                filename_dict = cursor.fetchone()
                while filename_dict is None:
                    #print(file_id)
                    #print(filename_select )
                    time.sleep(1)
                    cursor.execute(filename_select)
                    filename_dict = cursor.fetchone()
                    if keyboard.is_pressed("q"):
                        print("Exit")
                        break_flag = False
                        break
                filename = filename_dict['filename']
                print(filename)
                file_id = file_id + 1
                # чтение файла генератора и создание файла только с нужными эвентами
                read_and_write_file(f'{input_file_path_1}/{filename}.gen.txt', f'{output_file_path_1}/{filename}.{ending}.1.txt',
                                    f'{output_file_path_2}/{filename}.{ending}.2.txt')
                if os.path.exists(f'{output_file_path_1}/{filename}.{ending}.1.txt') and os.path.exists(f'{output_file_path_2}/{filename}.{ending}.2.txt'):
                    # запись в pipeline о том, что сплиттер обработал файл
                    file_id_select = "UPDATE pipeline SET stage_splitter = 'done' WHERE filename = %s" % filename
                    cursor.execute(file_id_select)
                    connection.commit()

                if keyboard.is_pressed("q") or break_flag is True:
                    print("Exit")
                    break
    finally:
        connection.close()
        print("#" * 20)
except Exception as ex:
    print("Connection refused...")
    print(ex)



