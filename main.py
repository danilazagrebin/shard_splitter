import pymysql
import os
from config import host, user, password, db_name, ending, shard, input_file_path_1, output_file_path_1
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
start = True
file_id = 1

def read_and_write_file(input_file_path, output_file_path):
    try:
        with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as outfile:
            for line in input_file:
                event_type = (line.split(',')[0])    # выбираем нужный параметр ивента
                event_type_num = int(event_type.split(' ')[1]) # убираем название параметра, оставляя само значение
                if shard == 1:                  # первый шард берет записи только с нечетными id
                    if event_type_num % 2 != 0:
                        outfile.write(line)
                if shard == 2:                  # второй шард берет записи только с четными id
                    if event_type_num % 2 == 0:
                        outfile.write(line)

        print(f"Content successfully written to {output_file_path}")

    except FileNotFoundError:
        print(f"Error: The file {input_file_path} does not exist.")

try:
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    print("successfully connected...")
    print("#" * 20)
    try:
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
                    file_id = file_id + 1
                cursor.execute(filename_select)
                filename_dict = cursor.fetchone()
                filename = filename_dict['filename']
                print(filename)
                # чтение файла генератора и создание файла только с нужными ивентами
                read_and_write_file(f'{input_file_path_1}/{filename}.gen.txt', f'{output_file_path_1}/{filename}.{ending}.{shard}.txt')
                if os.path.exists(f'{output_file_path_1}/{filename}.{ending}.{shard}.txt'):
                    # запись в pipeline о том, что сплиттер обработал файл
                    file_id_select = "UPDATE pipeline SET stage_splitter = 'done' WHERE filename = %s" % filename
                    cursor.execute(file_id_select)
                    connection.commit()
    finally:
        connection.close()
        print("#" * 20)
except Exception as ex:
    print("Connection refused...")
    print(ex)

