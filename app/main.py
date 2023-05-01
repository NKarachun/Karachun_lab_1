'''Варіант 6'''
'''Історія. 2020, 2021'''


from queries import *
from functions import *


import time


import psycopg2


if __name__ == '__main__':
    '''docker-compose build --no-cache && docker-compose up -d --force-recreate'''
    '''docker-compose up'''
    
    WORK = True
    while WORK:
        try:
            connect = psycopg2.connect(dbname='zno', user='postgres', password='postgres', host='db')

            with connect:
                start = time.time()
                cursor = connect.cursor()

                create = sql_create()
                cursor.execute(create)

                #-------------------------------------# 
                
                task(connect, cursor, 2020)

                #-------------------------------------# 

                task(connect, cursor, 2021)
                
                #-------------------------------------# 

                stop = time.time()
                create_time_file(start, stop, 'Time')

                #-------------------------------------# 

                create_result_file(cursor, 'Result')
        
                #-------------------------------------# 

                WORK = False

        except psycopg2.OperationalError as err:
            print(f'\nERROR: {err}\n')
            time.sleep(10)
        except psycopg2.errors.AdminShutdown as err:
            print(f'\nERROR: {err}\n')
        except psycopg2.InterfaceError as err:
            print(f'\nERROR: {err}\n')
        except FileNotFoundError as err:
            RUN_FLAG = False
            print(f'\nERROR: {err}\n')
            print(f'\nFile {err.filename} does not exist\n')