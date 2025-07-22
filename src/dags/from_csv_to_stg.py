from datetime import datetime
import vertica_python


from airflow import DAG
from airflow.operators.python_operator import PythonOperator


conn_info = {'host': 'vertica.tgcloudenv.ru',  # Адрес сервера из инструкции
             'port': 5433,
             'user': 'stv202506164',       # Полученный логин
             'password': 'pass',  # пароль.
                         # Если не любите хардкод, воспользуйтесь getpass()
                         # 'password': getpass(),
             'database': 'dwh',
             'tlsmode': 'disable',
             # Вначале он нам понадобится, а дальше — решите позже сами
                         'autocommit': True
             }


def clear_tables(conn_info=conn_info):
    # Рекомендуем использовать соединение:
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f'''
                    TRUNCATE TABLE STV202506164__STAGING.group_log;
                    TRUNCATE TABLE STV202506164__STAGING.groups;
                    TRUNCATE TABLE STV202506164__STAGING.dialogs;
                    TRUNCATE TABLE STV202506164__STAGING.users;

                    DROP TABLE IF EXISTS groups_rej;
                    DROP TABLE IF EXISTS dialogs_rej;
                    DROP TABLE IF EXISTS users_rej;
                    DROP TABLE IF EXISTS group_log_rej;
                    ''')

        res = cur.fetchall()
        return res


def load_to_stg(conn_info=conn_info, filename='filename'):
    # Рекомендуем использовать соединение:
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f'''
                COPY STV202506164__STAGING.{filename}
                FROM LOCAL '/data/{filename}.csv'
                DELIMITER ','
                REJECTED DATA AS TABLE STV202506164__STAGING.{filename}_rej                
                NULL AS ''
                SKIP 1  -- Пропустить заголовок, если есть
                ;
                    ''')

        res = cur.fetchall()
        return res


with DAG(
        'from_csv_to_stg',
        # Задаем расписание выполнения дага
        schedule_interval=None,
        description='upload_to_data',
        catchup=False,
        start_date=datetime.today()
) as dag:

    clear_tables = PythonOperator(
        task_id='clear_tables',
        python_callable=clear_tables,
        op_kwargs={
            'conn_info': conn_info
        },
    )

    from_csv_to_stg_groups = PythonOperator(
        task_id='from_csv_to_stg_groups',
        python_callable=load_to_stg,
        op_kwargs={
            'conn_info': conn_info,
            'filename': 'groups'
        },
    )

    from_csv_to_stg_users = PythonOperator(
        task_id='from_csv_to_stg_users',
        python_callable=load_to_stg,
        op_kwargs={
                'conn_info': conn_info,
                'filename': 'users'
        },
    )

    from_csv_to_stg_dialogs = PythonOperator(
        task_id='from_csv_to_stg_dialogs',
        python_callable=load_to_stg,
        op_kwargs={
                'conn_info': conn_info,
                'filename': 'dialogs'
        },
    )

    from_csv_to_stg_group_log = PythonOperator(
        task_id='from_csv_to_stg_group_log',
        python_callable=load_to_stg,
        op_kwargs={
                'conn_info': conn_info,
                'filename': 'group_log'
        },
    )

    (clear_tables >>
     [from_csv_to_stg_groups, from_csv_to_stg_users, from_csv_to_stg_dialogs, from_csv_to_stg_group_log])
