import mysql.connector

from author import Author
from author_of import Author_of
from book import Book
from ibookdb import IBOOKDB
from publisher import Publisher
from queryresult import QueryResult

class BOOKDB(IBOOKDB):

    def __init__(self,user,password,host,database,port):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.connection = None

    def initialize(self):
        self.connection = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            port=self.port
        )

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()

    def createTables(self):
        try:
            cursor = self.connection.cursor()

            # Tabloları oluşturmak için kullandığım SQL-DDL komutları , başarılı oluşan tabloların sayaç ile sayılması

            create_author_table = """
            CREATE TABLE IF NOT EXISTS author (
                author_id INT AUTO_INCREMENT PRIMARY KEY,
                author_name VARCHAR(60)
            )
            """
            create_publisher_table = """
            CREATE TABLE IF NOT EXISTS publisher (
                publisher_id INT AUTO_INCREMENT PRIMARY KEY,
                publisher_name VARCHAR(50)
            )
            """
            create_book_table = """
            CREATE TABLE IF NOT EXISTS book (
                isbn CHAR(13) PRIMARY KEY,
                book_name VARCHAR(120),
                publisher_id INT,
                first_publish_year CHAR(4),
                page_count INT,
                category VARCHAR(25),
                rating FLOAT,
                FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id)
            )
            """
            create_author_of_table = """
            CREATE TABLE IF NOT EXISTS author_of (
                isbn CHAR(13),
                author_id INT,
                FOREIGN KEY (isbn) REFERENCES book(isbn),
                FOREIGN KEY (author_id) REFERENCES author(author_id),
                PRIMARY KEY (isbn, author_id)
            )
            """
            create_phw1_table = """
            CREATE TABLE IF NOT EXISTS phw1 (
                isbn CHAR(13),
                book_name VARCHAR(120),
                rating FLOAT
            )
            """

            cursor.execute(create_author_table)
            cursor.execute(create_publisher_table)
            cursor.execute(create_book_table)
            cursor.execute(create_author_of_table)
            cursor.execute(create_phw1_table)

            table_names = ['author', 'publisher', 'book', 'author_of', 'phw1']
            success_counter = 0

            # Oluşması gereken tablolar için kontrol ve sayma adımı
            for table_name in table_names:
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if cursor.fetchone():
                    print(f"  {table_name} tablosu başarıyla oluşturuldu!")
                    success_counter += 1
                else:
                    print(f"{table_name} tablosu oluşturulamadı!")
            print("  -------------------------------------")
            print(f"  {success_counter} tablo başarıyla oluşturuldu!")
            self.connection.commit()
            cursor.close()
            return success_counter
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return 0

    def dropTables(self):
        try:
            cursor = self.connection.cursor()

            # Tabloları silebilmek için dış anahtar kısıtlamalarını geçici olarak devre dışı bırakıyoruz
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            # Tabloları silmek için kullanılan SQL sorguları
            drop_author_of_table = "DROP TABLE IF EXISTS author_of"
            drop_phw1_table = "DROP TABLE IF EXISTS phw1"
            drop_book_table = "DROP TABLE IF EXISTS book"
            drop_author_table = "DROP TABLE IF EXISTS author"
            drop_publisher_table = "DROP TABLE IF EXISTS publisher"

            cursor.execute(drop_author_of_table)
            cursor.execute(drop_phw1_table)
            cursor.execute(drop_book_table)
            cursor.execute(drop_author_table)
            cursor.execute(drop_publisher_table)

            # Dış anahtar kısıtlamalarını etkinleştirme
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

            self.connection.commit()
            cursor.close()
            print("Tablolar silindi!")
            return 5
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return 0

    def insertPublisher(self, publishers: list[Publisher]) -> int:
        try:
            cursor = self.connection.cursor()

            # Txt'den gelen bilgiler ile Yayınevlerini tabloya eklemek için SQL sorguları
            insert_query = "INSERT INTO publisher (publisher_id, publisher_name) VALUES (%s, %s)"

            for i, publisher in enumerate(publishers, start=1):
                cursor.execute(insert_query, (publisher.publisher_id, publisher.publisher_name))

            self.connection.commit()
            cursor.close()
            print("Yayınevleri Eklendi!")
            return len(publishers)
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return 0

    def insertAuthor(self, authors: list[Author]) -> int:
        try:
            cursor = self.connection.cursor()
            insert_query = "INSERT INTO author (author_id, author_name) VALUES (%s, %s)"
            for i, author in enumerate(authors, start=1):
                cursor.execute(insert_query, (author.author_id, author.author_name))
            self.connection.commit()
            cursor.close()
            print("Yazarlar Eklendi!")
            return len(authors)
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return 0

    def insertBook(self, books: list[Book]) -> int:
        try:
            cursor = self.connection.cursor()
            insert_query = "INSERT INTO book (isbn, book_name, publisher_id, first_publish_year, page_count, category, rating) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            for i, book in enumerate(books, start=1):
                cursor.execute(insert_query, (
                    book.isbn, book.book_name, book.publisher_id, book.first_publish_year, book.page_count, book.category,
                    book.rating))
            self.connection.commit()
            cursor.close()
            print("Kitaplar Eklendi!")
            return len(books)
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return 0

    def insertAuthor_of(self, author_ofs: list[Author_of]) -> int:
        try:
            cursor = self.connection.cursor()
            insert_query = "INSERT INTO author_of (isbn, author_id) VALUES (%s, %s)"

            for author_of in author_ofs:
                cursor.execute(insert_query, (author_of.isbn, author_of.author_id))

            self.connection.commit()
            cursor.close()
            print("Kitap Yazarları Eklendi!")
            return len(author_ofs)
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return 0

    def functionQ1(self) -> list[QueryResult.ResultQ1]:
        try:
            cursor = self.connection.cursor()
            # En fazla sayfaya sahip olan kitabın bilgilerini almak için SQL sorgusu
            query = """
            SELECT b.isbn, b.first_publish_year, b.page_count, p.publisher_name
            FROM book AS b
            JOIN publisher AS p ON b.publisher_id = p.publisher_id
            WHERE b.page_count = (SELECT MAX(page_count) FROM book)
            ORDER BY b.isbn ASC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            q1_results = []
            for result in results:
                q1_result = QueryResult.ResultQ1(result[0], result[1], result[2], result[3])
                q1_results.append(q1_result)
            cursor.close()
            return q1_results
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ2(self, author_id1: int, author_id2: int) -> list[QueryResult.ResultQ2]:
        try:
            cursor = self.connection.cursor()

            # Verilen iki yazarın birlikte yazdığı kitapları yayımlayan yayınevlerinin
            # publisher id'lerini ve bu yayınevlerinin yayımladığı tüm kitapların ortalama sayfa sayısını (page count) sorgusu
            query = """
                SELECT publisher.publisher_id, AVG(book.page_count) AS avg_page_count
                FROM publisher
                JOIN book ON publisher.publisher_id = book.publisher_id
                JOIN author_of ON book.isbn = author_of.isbn
                WHERE author_of.author_id IN (%s, %s)
                GROUP BY publisher.publisher_id
                ORDER BY publisher.publisher_id ASC
            """
            cursor.execute(query, (author_id1, author_id2))
            results = cursor.fetchall()
            query_results = []
            for result in results:
                publisher_id = result[0]
                avg_page_count = result[1]
                query_results.append(QueryResult.ResultQ2(publisher_id, avg_page_count))

            cursor.close()
            return query_results

        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ3(self, author_name: str) -> list[QueryResult.ResultQ3]:
        try:
            cursor = self.connection.cursor()

            # Verilen yazar adına sahip yazarın en erken yayımlanan kitabının bilgilerini sorgulayın
            query = """
                SELECT book.book_name, book.category, book.first_publish_year
                FROM book
                JOIN author_of ON book.isbn = author_of.isbn
                JOIN author ON author_of.author_id = author.author_id
                WHERE author.author_name = %s
                ORDER BY book.book_name ASC, book.category ASC, book.first_publish_year ASC
            """
            cursor.execute(query, (author_name,))
            results = cursor.fetchall()
            query_results = []
            for result in results:
                book_name = result[0]
                category = result[1]
                first_publish_year = result[2]
                query_results.append(QueryResult.ResultQ3(book_name, category, first_publish_year))

            cursor.close()
            return query_results

        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ4(self) -> list[QueryResult.ResultQ4]:
        try:
            cursor = self.connection.cursor()
            # En az 3 kelime içeren isimlere sahip yayınevlerini ve kitap sayılarını sorgulayın
            query = """
                SELECT 
                    p.publisher_id,
                    b.category
                FROM 
                    book b
                INNER JOIN publisher p ON b.publisher_id = p.publisher_id
                INNER JOIN (
                    SELECT 
                        publisher_id,
                        COUNT(*) AS total_books,
                        AVG(rating) AS avg_rating
                    FROM 
                        book
                    GROUP BY 
                        publisher_id
                    HAVING 
                        COUNT(*) >= 3 AND AVG(rating) > 3
                ) AS filtered_publishers ON b.publisher_id = filtered_publishers.publisher_id
                WHERE 
                    p.publisher_name LIKE '% % %'  -- En az 3 kelime içeren isimler
                GROUP BY 
                    p.publisher_id, b.category
                ORDER BY 
                    p.publisher_id ASC, b.category ASC;
            """

            cursor.execute(query)
            results = cursor.fetchall()
            query_results = []
            for result in results:
                publisher_id = result[0]
                category = result[1]
                query_results.append(QueryResult.ResultQ4(publisher_id, category))

            cursor.close()
            return query_results

        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ5(self, author_id: int) -> list[QueryResult.ResultQ5]:
        try:
            cursor = self.connection.cursor()
            # Verilen author_id'nin çalıştığı yayınevleriyle çalışmış yazarları sorgulayın
            query = """
            SELECT DISTINCT a.author_id, a.author_name
            FROM author_of ao
            INNER JOIN author a ON ao.author_id = a.author_id
            INNER JOIN book b ON ao.isbn = b.isbn
            WHERE b.publisher_id IN (
                SELECT DISTINCT publisher_id
                FROM book
                WHERE isbn IN (
                    SELECT isbn
                    FROM author_of
                    WHERE author_id = %s
                )
            )
            ORDER BY a.author_id ASC
            """
            cursor.execute(query, (author_id,))
            results = cursor.fetchall()

            query_results = [QueryResult.ResultQ5(author_id=result[0], author_name=result[1]) for result in results]

            cursor.close()
            return query_results
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ6(self) -> list[QueryResult.ResultQ6]:
        try:
            cursor = self.connection.cursor()
            # Yalnızca kendi kitaplarını yayımlayan yazarları bulmak için sorgu
            query = """
            SELECT DISTINCT a.author_id, b.isbn
            FROM author_of ao
            INNER JOIN book b ON ao.isbn = b.isbn
            INNER JOIN author a ON ao.author_id = a.author_id
            WHERE b.publisher_id NOT IN (
                SELECT DISTINCT publisher_id
                FROM book
                WHERE isbn NOT IN (
                    SELECT isbn
                    FROM author_of
                    WHERE author_id = a.author_id
                )
            )
            ORDER BY a.author_id, b.isbn
            """
            cursor.execute(query)
            results = cursor.fetchall()

            query_results = []
            for result in results:
                author_id = result[0]
                isbn = result[1]
                query_results.append(QueryResult.ResultQ6(author_id, isbn))

            cursor.close()
            return query_results

        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ7(self, rating: float) -> list[QueryResult.ResultQ7]:
        try:
            cursor = self.connection.cursor()

            query = """
            SELECT publisher.publisher_id, publisher.publisher_name
            FROM publisher
            INNER JOIN book ON publisher.publisher_id = book.publisher_id
            WHERE book.category = 'Roman'
            GROUP BY publisher.publisher_id, publisher.publisher_name
            HAVING COUNT(book.isbn) >= 2 AND AVG(book.rating) > %s
            ORDER BY publisher.publisher_id
            """
            cursor.execute(query, (rating,))
            results = cursor.fetchall()
            query_results = []
            for result in results:
                publisher_id = result[0]
                publisher_name = result[1]
                query_results.append(QueryResult.ResultQ7(publisher_id, publisher_name))

            cursor.close()
            return query_results
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ8(self) -> list[QueryResult.ResultQ8]:
        try:
            cursor = self.connection.cursor()

            # Aynı isme sahip olan kitapların en küçük rating'e sahip olanlarını bulma
            query = """
            SELECT b1.isbn, b1.book_name, b1.rating
            FROM book b1
            WHERE b1.book_name IN (
                SELECT b2.book_name
                FROM book b2
                GROUP BY b2.book_name
                HAVING COUNT(*) > 1
            )
            AND b1.rating = (
                SELECT MIN(b3.rating)
                FROM book b3
                WHERE b3.book_name = b1.book_name
            )

            """

            # Kitapları phw1 tablosuna toplu olarak ekleme
            bulk_insert_query = """
            INSERT INTO phw1 (isbn, book_name, rating)
            VALUES (%s, %s, %s)
            """

            # En küçük rating'e sahip olan kitapları bulma
            cursor.execute(query)

            # Toplu ekleme işlemi için verileri toplama
            books_to_insert = cursor.fetchall()

            # Toplu ekleme işlemi
            cursor.executemany(bulk_insert_query, books_to_insert)

            # Değişiklikleri onaylama
            self.connection.commit()

            # phw1 tablosundaki tüm satırların isbn, book name ve rating bilgilerini alalım
            cursor.execute("SELECT isbn, book_name, rating FROM phw1 ORDER BY isbn")

            results = cursor.fetchall()

            query_results = []
            for result in results:
                isbn = result[0]
                book_name = result[1]
                rating = result[2]
                query_results.append(QueryResult.ResultQ8(isbn, book_name, rating))

            cursor.close()
            return query_results
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return []

    def functionQ9(self, keyword: str) -> float:
        try:
            cursor = self.connection.cursor()

            # Kitap isimlerinin içinde anahtar kelimeyi içeren kayıtların rating'lerini 1 artıralım
            update_query = """
            UPDATE book
            SET rating = LEAST(rating + 1, 5)
            WHERE book_name LIKE %s AND rating < 4
            """
            cursor.execute(update_query, ('%' + keyword + '%',))

            # Güncelleme işleminden sonra tüm kitapların rating'lerinin toplamını alalım
            total_rating_query = """
            SELECT SUM(rating)
            FROM book
            """
            cursor.execute(total_rating_query)
            total_rating = cursor.fetchone()[0]

            self.connection.commit()
            cursor.close()
            return total_rating
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return -1

    def function10(self) -> int:
        try:
            cursor = self.connection.cursor()

            # Henüz hiç kitap yayımlamamış yayınevlerini silelim
            delete_query = """
            DELETE FROM publisher
            WHERE publisher_id NOT IN (
                SELECT DISTINCT publisher_id
                FROM book
            )
            """
            cursor.execute(delete_query)

            # Silme işleminden sonra yayınevleri tablosundaki kayıt sayısını alalım
            count_query = """
            SELECT COUNT(*)
            FROM publisher
            """
            cursor.execute(count_query)
            publisher_count = cursor.fetchone()[0]

            self.connection.commit()
            cursor.close()
            return publisher_count
        except mysql.connector.Error as err:
            print(f"Hata: {err}")
            return -1

