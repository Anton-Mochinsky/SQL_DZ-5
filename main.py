import psycopg2
from pprint import pprint

def creat_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        name VARCHAR(20),
        lastname VARCHAR(40),
        email VARCHAR(254)
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phone_number(
        number VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
        );
    """)
    return

def delete_bd(cur):
    cur.execute("""
    DROP TABLE clients, phone_number CASCADE;
    """)

def add_phones(cur, client_id, phones):
    cur.execute("""
    INSERT INTO phone_number(number, client_id)
    VALUES (%s, %s)
    """,(phones,client_id))
    return client_id

def add_client(cur, name=None, last_name=None, email=None, phones=None):
    cur.execute("""
    INSERT INTO clients(name, lastname, email)
    VALUES (%s, %s, %s)
    """, (name, last_name, email))
    cur.execute("""
    SELECT id FROM clients
    ORDER BY id DESC
    LIMIT 1
    """)
    id = cur.fetchone()[0]
    if phones is None:
        return id
    else:
        add_phones(cur, id, phones)
        return id

def change_client(cur, client_id, first_name=None, last_name=None, email=None):
    cur.execute("""
           SELECT * from clients
           WHERE id = %s
           """, (client_id,))
    info = cur.fetchone()
    if first_name is None:
        first_name = info[1]
    if last_name is None:
        last_name = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
           UPDATE clients
           SET name = %s, lastname = %s, email =%s 
           where id = %s
           """, (first_name, last_name, email, client_id))
    return client_id

def delete_phone(cur, phones):
    cur.execute("""
        DELETE FROM phone_number 
        WHERE number = %s
        """, (phones, ))
    return phones

def delete_client(cur, id):
    cur.execute("""
        DELETE FROM phone_number
        WHERE client_id = %s
        """, (id, ))
    cur.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id,))
    return id

def find_client(cur, name=None, last_name=None, email=None, number=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if last_name is None:
        last_name = '%'
    else:
        last_name = '%' + last_name + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if number is None:
        cur.execute("""
            SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
            LEFT JOIN phone_number p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s
            """, (name, last_name, email))
    else:
        cur.execute("""
            SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
            LEFT JOIN phone_number p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s AND p.number like %s
            """, (name, last_name, email, number))
    return cur.fetchall()

conn = psycopg2.connect(database="DZ_SQL", user="postgres", password="123321123")
with conn.cursor() as curs:
    # Удаление бд перед заполнением
    delete_bd(curs)
    # Создание бд
    creat_db(curs)
    print('Бд создана')
    # Добавление в бд данных о пользователях
    print("Добавление данных id: ",
          add_client(curs, "Антон", "Мочинский", "avm50001@yandex.ru", 89819864408))
    print("Добавление данных id: ",
          add_client(curs, "Игорь", "Фазинский", "faz_eq@yandex.ru"))
    print("Добавление данных id: ",
          add_client(curs, "Николай", "Бредлеев", "Berd123@gmail.com", 93215845521))
    print("Добавление данных id: ",
          add_client(curs, "Илья", "Злобин", "Zlobin32@yandex.ru"))
    print("Добавление данных id: ",
          add_client(curs, "Егор", "Панасенко", "Eva@mail.ru", 86588541452))
    print("Добавление данных id: ",
          add_client(curs, "Лев", "Канакин", "Lego_word@yandex.ru"))
    print('Данные в талицах')
    curs.execute("""
    SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
    LEFT JOIN phone_number p ON c.id = p.client_id
    ORDER BY c.id
    """)
    pprint(curs.fetchall())
    # Изменение данных клиента
    print("Изменены данные клиента id: ",
          change_client(curs, 4, "Виктор", 'Фикусов', 'frikadelka@mail.com'))
    # Удаление телефонного номера
    print("Телефон удалён c номером: ",
          delete_phone(curs, '86588541452'))
    print("Данные в таблицах")
    curs.execute("""
                    SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
                    LEFT JOIN phone_number p ON c.id = p.client_id
                    ORDER by c.id
                    """)
    pprint(curs.fetchall())
    # Удалим клиента №4
    print("Клиент удалён с id: ",
          delete_client(curs, 4))
    curs.execute("""
                               SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
                               LEFT JOIN phone_number p ON c.id = p.client_id
                               ORDER by c.id
                               """)
    pprint(curs.fetchall())
    # Проведем поиск в бд по различным значениям
    print('Найденный клиент по имени:')
    pprint(find_client(curs, 'Антон'))

    print('Найденный клиент по email:')
    pprint(find_client(curs, None, None, 'Lego_word@yandex.ru'))

    print('Найденный клиент по имени, фамилии и email:')
    pprint(find_client(curs, 'Антон', 'Мочинский',
                       'avm50001@yandex.ru'))

    print('Найденный клиент по имени, фамилии, телефону и email:')
    pprint(find_client(curs, 'Антон', 'Мочинский',
                       'avm50001@yandex.ru', '89819864408'))

    print('Найденный клиент по имени, фамилии, телефону:')
    pprint(find_client(curs, None, None, None, '93215845521'))

    conn.commit()
conn.close()
