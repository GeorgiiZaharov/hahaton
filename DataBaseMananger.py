import pandas as pd
import numpy as np
import asyncio
import aiomysql
import logging

logging.basicConfig(level=logging.DEBUG)

async def create_table(cursor):
    try:
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS `OKPD2` (
                `okpd2` VARCHAR(255),
                `okpd2_name` VARCHAR(255)
            )
        ''')

        logging.info("Table OKPD2 created successfully or already exists.")
    except Exception as e:
        logging.error(f"Error creating table OKPD2: {e}")

class MTRDTO:
    def __init__(self, SKMTR: str, name: str, labeling: str, regulations: str, parameters: str, basic_unit: str, okpd2: str):
        self.SKMTR = SKMTR  # код СКМТР
        self.Name = name  # Наименование
        self.Marking = labeling  # Маркировка
        self.Rfgulations = regulations  # Регламенты
        self.Parameters = parameters  # Параметры
        self.Base_unit = basic_unit  # Базисная единица измерения
        self.OKPD2 = okpd2  # ОКПД2

    def __repr__(self):
        return f"MTRDTO(SKMTR={self.SKMTR}, name={self.name}, marking={self.marking}, regulations={self.regulations}, parameters={self.parameters}, base_unit={self.base_unit}, okpd2={self.okpd2})"


class OldDataBaseManager:

    def __init__(self, db_config, excel_path):

        self.db_config = {
            'user': 'root',
            'password': 'Benzogang1!',  # Ensure this is the correct password
            'host': 'localhost',
            'port': 3306,
            'db': 'oldbase'
        }
        self.excel_path = excel_path

    async def Build(self):

        logging.info('Connecting to the MySQL database...')
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            # Reading data from Excel file
            logging.info('Reading data from Excel file: %s', self.excel_path)
            df = pd.read_excel(self.excel_path)

            # Replace NaN with None to insert as NULL in the database
            df = df.replace({np.nan: None})

            # Create table in the MySQL database
            logging.info('Creating MTR table if it does not exist...')
            await cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS MTR (
    SKMTR VARCHAR(50),
    name TEXT,
    labeling TEXT,
    regulations TEXT,
    parameters TEXT,
    basic_unit VARCHAR(50),
    okpd2 VARCHAR(50)
);

            ''')

            # Insert data into the table
            logging.info('Inserting data into the MTR table...')
            for _, row in df.iterrows():
                await cursor.execute('''
                    INSERT INTO MTR (SKMTR, Name, Labeling, Regulations, Parameters, Basic_unit, OKPD2)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                ''', (row['код СКМТР'], row['Наименование'], row['Маркировка'], row['Регламенты (ГОСТ/ТУ)'],
                      row['Параметры'], row['Базисная Единица измерения'], row['ОКПД2']))

            await conn.commit()
            logging.info('Data inserted successfully.')

        conn.close()
        logging.info('Connection to MySQL closed.')

    async def GetMTR(self):
        logging.info('Fetching all data from MTR table...')
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT * FROM MTR;")
            rows = await cursor.fetchall()

            # Convert each record to DTO and return an async generator
            for row in rows:
                yield MTRDTO(**row)

        conn.close()
        logging.info('Connection to MySQL closed.')


# NewDataBaseManager with logging added

class NewDataBaseManager:
    def __init__(self, db_config):
        self.db_config = db_config
        self.excel_path1=r'C:\hahaton-master\OKPD_2.xlsx'


    async def Build(self):
        logging.info('Connecting to the MySQL database...')
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            logging.info('Creating `GROUPS` and MTR tables if they do not exist...')
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS `GROUPS` (
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
                );
            """)
            logging.info('Creating `GROUPS` and MTR tables if they do not exist...')
            await cursor.execute('''
                CREATE TABLE IF NOT EXISTS `OKPD2` (
                    `okpd2` VARCHAR(255),
                    `okpd2_name` VARCHAR(255)
                )
            ''')
            logging.info('Creating `GROUPS` and MTR tables if they do not exist...')
            await cursor.execute('''
                CREATE TABLE IF NOT EXISTS MTR (
                    SKMTR VARCHAR(255) PRIMARY KEY,
                    Name VARCHAR(255),
                    Marking VARCHAR(255),
                    Regulations VARCHAR(255),
                    Parameters VARCHAR(255),
                    Base_unit VARCHAR(255),
                    OKPD2 VARCHAR(255),
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
                );
            ''')

            logging.info('Creating GROUPS and MTR tables if they do not exist...')

            logging.info('Reading data from Excel file: %s', self.excel_path1)
            df = pd.read_excel(self.excel_path1)

            # Replace NaN with None to insert as NULL in the database
            df = df.replace({np.nan: None})

            # Create table in the MySQL database
            logging.info('Creating OKPD2 table if it does not exist...')
            for _, row in df.iterrows():
                await cursor.execute('''
                    INSERT INTO OKPD2 (okpd2,okpd2_NAME)
                    VALUES (%s, %s);
                ''', (row['OKPD2'], row['OKPD2_NAME']))

            logging.info('Tables created successfully.')

            await conn.commit()

        conn.close()

    async def InsertRowMTR(self, row_dict):
        logging.info('Начинается вставка строки в таблицу MTR...')
        try:
            # Установка соединения
            conn = await aiomysql.connect(**self.db_config)

            async with conn.cursor() as cursor:
                # Формирование SQL-запроса для вставки данных
                columns = ', '.join(row_dict.keys())
                placeholders = ', '.join(['%s'] * len(row_dict))
                print('ZHOPA',placeholders,columns)
                sql = f'INSERT INTO MTR ({columns}) VALUES ({placeholders})'

                logging.debug(f'SQL запрос: {sql}')  # Логируем SQL запрос для проверки

                # Выполнение запроса
                await cursor.execute(sql, tuple(row_dict.values()))
                await conn.commit()  # Подтверждаем транзакцию

                logging.info('Строка успешно вставлена в таблицу MTR.')


        except aiomysql.MySQLError as e:
            # Логируем ошибки, если что-то пошло не так
            logging.error(f'Ошибка вставки данных в MTR: {e}')

        finally:
            # Закрытие соединения
            if conn:
                conn.close()
                logging.info('Соединение с базой данных закрыто.')

    async def GetChildrensByOKPD2(self, okpd2)->dict:
        logging.info('Fetching groups by OKPD2: %s', okpd2)
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT group_id, group_name FROM GROUPS WHERE okpd2_parent = %s', (okpd2,))
            rows = await cursor.fetchall()
            result = {row[0]: row[1] for row in rows}

        conn.close()
        logging.info('Groups fetched successfully.')
        return result

    async def InsertRowInGroups(self, row_dict):

        logging.info('Inserting row into GROUPS table...')
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            columns = ', '.join(row_dict.keys())
            placeholders = ', '.join(['%s'] * len(row_dict))

            sql = f'INSERT INTO GROUPS ({columns}) VALUES ({placeholders})'
            logging.debug(f'SQL Query: {sql}')
            logging.debug(f'Values: {tuple(row_dict.values())}')
            try:
                await cursor.execute(sql, tuple(row_dict.values()))
                await conn.commit()
                logging.info('Row inserted successfully.')
            except Exception as e:
                logging.error(f'Error while inserting new note: {e}')

        conn.close()

    # Additional methods follow the same pattern...

    async def GetPropertiesByGroupID(self, group_id):
        logging.info('Fetching properties by group ID: %s', group_id)
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            await cursor.execute('''
                    SELECT property1, property2, property3, property4, property5, 
                           property6, property7, property8, property9, property10 
                    FROM GROUPS WHERE group_id = %s
                ''', (group_id,))
            row = await cursor.fetchone()
        conn.close()
        logging.info('Properties fetched successfully.')
        return row if row else []

    async def GetSubClasses(self, okpd2_class, length)->dict:
        logging.info('Fetching subclasses by OKPD2 class: %s, length: %s', okpd2_class, length)
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            like_pattern = f'{okpd2_class}%'
            await cursor.execute('''
                SELECT okpd2, okpd2_name 
                FROM OKPD2 
                WHERE okpd2 LIKE %s AND CHAR_LENGTH(okpd2) = %s
            ''', (like_pattern, length))
            rows = await cursor.fetchall()

            result = {row[1]: row[0] for row in rows}

        conn.close()
        logging.info('Subclasses fetched successfully.')

        return result

    async def GetRoots(self):
        logging.info('Fetching root classes...')
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:
            await cursor.execute('''
                SELECT okpd2, okpd2_name 
                FROM OKPD2 
                WHERE CHAR_LENGTH(okpd2) = 2
            ''')
            rows = await cursor.fetchall()
            result = {row[1]: row[0] for row in rows}
        conn.close()
        logging.info('Root classes fetched successfully.')
        return result

    async def GetValueByOKPD2(self, okpd2):
        # Asynchronously get the string representation of OKPD2
        conn = await aiomysql.connect(**self.db_config)
        async with conn.cursor() as cursor:

            await cursor.execute('SELECT okpd2_name FROM OKPD2 WHERE okpd2 = %s', (okpd2,))
            row = await cursor.fetchone()

        conn.close()
        return row[0] if row else []
