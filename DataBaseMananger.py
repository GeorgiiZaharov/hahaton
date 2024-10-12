import pandas as pd
import numpy as np
import asyncio
import aiomysql

class MTRDTO:
    def __init__(self, **entries):
        self.__dict__.update(entries)

class OldDataBaseManager:
    def __init__(self, db_config, excel_path):
        self.db_config = db_config
        self.excel_path = excel_path

    async def Build(self):
        # Подключение к MySQL
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            # Чтение данных из файла Excel
            df = pd.read_excel(self.excel_path)

            # Заменяем NaN на None, чтобы корректно вставлялись как NULL в базу данных
            df = df.replace({np.nan: None})

            # Создание таблицы в базе данных MySQL
            await cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS MTR (
                    код_СКМТР VARCHAR(50),
                    Наименование TEXT,
                    Маркировка TEXT,
                    Регламенты TEXT,
                    Параметры TEXT,
                    Базисная_Единица_измерения VARCHAR(50),
                    ОКПД2 VARCHAR(50)
                );
            ''')

            # Вставка данных в таблицу
            for _, row in df.iterrows():
                await cursor.execute(''' 
                    INSERT INTO MTR (код_СКМТР, Наименование, Маркировка, Регламенты, Параметры, Базисная_Единица_измерения, ОКПД2)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                ''', (row['код СКМТР'], row['Наименование'], row['Маркировка'], row['Регламенты (ГОСТ/ТУ)'],
                      row['Параметры'], row['Базисная Единица измерения'], row['ОКПД2']))

            await conn.commit()

        conn.close()

    async def GetMTR(self):
        # Подключение к MySQL
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # Получение всех данных из таблицы
            await cursor.execute("SELECT * FROM MTR;")
            rows = await cursor.fetchall()

            # Преобразование каждой записи в DTO и возврат асинхронного генератора
            for row in rows:
                yield MTRDTO(**row)

        conn.close()

# Параметры для подключения к MySQL
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Benzogang1!',  # Замените на ваш пароль
    'db': 'oldbase'  # Имя базы данных
}

# Путь к файлу Excel
excel_path = 'MTR.xlsx'

import aiomysql
import asyncio


class NewDataBaseManager:
    def __init__(self, db_config):
        # Initialize the class with the database configuration
        self.db_config = db_config

    async def Build(self):
        # Asynchronously create the tables in the database
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            # Create the GROUPS table if it does not exist
            await cursor.execute('''
                CREATE TABLE IF NOT EXISTS GROUPS (
                    group_id INT PRIMARY KEY AUTO_INCREMENT,
                    group_name VARCHAR(255),
                    okpd2_parent VARCHAR(255),
                    property1 VARCHAR(255),
                    property2 VARCHAR(255),
                    property3 VARCHAR(255),
                    property4 VARCHAR(255),
                    property5 VARCHAR(255),
                    property6 VARCHAR(255),
                    property7 VARCHAR(255),
                    property8 VARCHAR(255),
                    property9 VARCHAR(255),
                    property10 VARCHAR(255)
                )
            ''')
            # Create the MTR table if it does not exist
            await cursor.execute('''
                CREATE TABLE IF NOT EXISTS MTR (
                    skmtr_code VARCHAR(255) PRIMARY KEY,
                    name VARCHAR(255),
                    marking VARCHAR(255),
                    regulations VARCHAR(255),
                    parameters VARCHAR(255),
                    base_unit VARCHAR(255),
                    okpd2 VARCHAR(255),
                    group_id INT,
                    property1 VARCHAR(255),
                    property2 VARCHAR(255),
                    property3 VARCHAR(255),
                    property4 VARCHAR(255),
                    property5 VARCHAR(255),
                    property6 VARCHAR(255),
                    property7 VARCHAR(255),
                    property8 VARCHAR(255),
                    property9 VARCHAR(255),
                    property10 VARCHAR(255)
                )
            ''')
            await conn.commit()
        conn.close()

    async def InsertRowMTR(self, row_dict):
        # Asynchronously add a row to the MTR table
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            columns = ', '.join(row_dict.keys())
            placeholders = ', '.join(['%s'] * len(row_dict))
            sql = f'INSERT INTO MTR ({columns}) VALUES ({placeholders})'
            await cursor.execute(sql, tuple(row_dict.values()))
            await conn.commit()
        conn.close()

    async def GetChildrensByOKPD2(self, okpd2):
        # Asynchronously get groups by the given OKPD2
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT group_id, group_name FROM GROUPS WHERE okpd2_parent = %s', (okpd2,))
            rows = await cursor.fetchall()
            result = {}
            for row in rows:
                result[row[0]] = row[1]  # Добавляем пары group_id: group_name в общий словарь
        conn.close()
        return result

    async def InsertRowInGroups(self, row_dict):
        # Asynchronously add a row to the GROUPS table
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            columns = ', '.join(row_dict.keys())
            placeholders = ', '.join(['%s'] * len(row_dict))
            sql = f'INSERT INTO GROUPS ({columns}) VALUES ({placeholders})'
            await cursor.execute(sql, tuple(row_dict.values()))
            await conn.commit()
        conn.close()

    async def GetValueByOKPD2(self, okpd2):
        # Asynchronously get the string representation of OKPD2
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT okpd2_name FROM OKPD2 WHERE okpd2 = %s', (okpd2,))
            row = await cursor.fetchone()
        conn.close()
        return row[0] if row else None


    async def GetPropertiesByGroupID(self, group_id):
        # Asynchronously get properties by group ID
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            await cursor.execute('''
                    SELECT property1, property2, property3, property4, property5, 
                           property6, property7, property8, property9, property10 
                    FROM GROUPS WHERE group_id = %s
                ''', (group_id,))
            row = await cursor.fetchone()
        conn.close()
        return row if row else []


    async def GetSubClasses(self, okpd2_class, length):
        # Asynchronously get subclasses by the given OKPD2 class and length
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            like_pattern = f'{okpd2_class}%'  # Pattern for LIKE clause
            await cursor.execute('''
                SELECT okpd2, okpd2_name 
                FROM OKPD2 
                WHERE okpd2 LIKE %s AND CHAR_LENGTH(okpd2) = %s
            ''', (like_pattern, length))
            rows = await cursor.fetchall()
            result = {row[1]: row[0] for row in rows}
        conn.close()
        return result

    async def GetRoots(self):
        # Asynchronously get root classes (OKPD2 with 2 characters)
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            await cursor.execute('''
                SELECT okpd2, okpd2_name 
                FROM OKPD2 
                WHERE CHAR_LENGTH(okpd2) = 2
            ''')
            rows = await cursor.fetchall()
            result = [{row[1]: row[0]} for row in rows]
        conn.close()
        return result
