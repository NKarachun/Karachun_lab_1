import queries
import csv


def correct_type(string):
    if string != 'null' and string is not None:
        return float(string.replace(',', '.'))


def task(connect, cursor, year):
    if year == 2020:
        path = r'./Odata2020File.csv'
        encoding = 'windows-1251'
    elif year == 2021:
        path = r'./Odata2021File.csv'
        encoding = 'utf-8-sig'

    select = queries.sql_select('zno', year)
    cursor.execute(select)
    count = cursor.fetchone()[0]
    print(f'{count} rows before insert')
    with open(path, 'r', encoding=encoding) as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        headers = next(reader)
        headers.append('YEAR')  #  Список назв колонок
        str_headers = ', '.join(headers)  #  Строка назв колонок
        idx_float = [headers.index(i) for i in headers if 'Ball100' in i]  #  Індекси float-колонок
        for row_id, row in enumerate(reader):
            if row_id >= int(count):
                tmp_row = []
                for el_id, el in enumerate(row):
                    if el_id in idx_float:
                        el = correct_type(el)
                        tmp_row.append(el)
                    else:
                        tmp_row.append(el)
                lst_row = list(map(str, tmp_row))
                lst_row.append(str(year))
                lst_row = [x.replace("\'", "`") for x in lst_row]
                tmp_str = ''
                for i in lst_row:
                    tmp_str += "\'" + i + "\', "
                str_row = tmp_str[:-2]
                str_row = str_row.replace("\'null\'", "null")
                str_row = str_row.replace("\'None\'", "null")
                str_insert = queries.sql_insert(str_headers, str_row)
                cursor.execute(str_insert)
                if (row_id+1) % 1000 == 0 and row_id != 0:
                    print(f'{row_id+1} rows in table where year={year}')
                    connect.commit()

        connect.commit()


def create_time_file(start, stop, name):
    with open(f'{name}.txt', 'w') as timefile:
        timefile.write(f'Execution time: {round(stop - start, 0)} s')


def create_result_file(cursor, name):
    with open(f'{name}.csv', 'w', newline='', encoding='utf-8') as csvfile:
        query = queries.sql_variant()
        cursor.execute(query)
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow([col[0] for col in cursor.description])

        for row in cursor:
            writer.writerow([str(el) for el in row])
