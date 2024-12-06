import mysql.connector
from mysql.connector import Error


class DatabaseInserter:
    def __init__(self, db_config):
        """Initialize with database configuration."""
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish a connection to the database."""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Database connection established.")
        except Error as e:
            print(f"Error connecting to database: {e}")

    def disconnect(self):
        """Close the database connection."""
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            print("Database connection closed.")

    def bulk_insert_data(self, table_name, fields, data):
        """Insert data into a table using bulk insert."""
        try:
            query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({', '.join(['%s'] * len(fields))})"
            self.cursor.executemany(query, data)
            print(f"Data inserted into {table_name} successfully.")
        except Error as e:
            print(f"Error inserting into {table_name}: {e}")

    def commit_changes(self):
        """Commit the changes to the database."""
        try:
            self.conn.commit()
            print("Changes committed to the database.")
        except mysql.connector.Error as e:
            print(f"Error committing changes: {e}")


if __name__ == "__main__":
    # Database connection configuration
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'root',  # Update this as per your MySQL setup
        'database': 'Bookstore'
    }

    inserter = DatabaseInserter(db_config)
    inserter.connect()

    # Sample data for each table
    users_data = [
        (1, 'John', 'Doe', 'john.doe@example.com', '1234567890', '123 Elm St, NY', '2023-01-01', 500.00, 'Admin'),
        (2, 'Jane', 'Smith', 'jane.smith@example.com', '9876543210', '456 Oak St, CA', '2023-02-15', 300.00, 'User'),
        (3, 'Emily', 'Johnson', 'emily.j@example.com', '1112223333', '789 Pine St, TX', '2023-03-20', 150.00, 'User'),
    ]

    books_data = [
        (1, '9781234567890', 'Book A', 'Publisher A', 2022, '1st', 15.99, 50),
        (2, '9780987654321', 'Book B', 'Publisher B', 2021, '2nd', 25.50, 20),
        (3, '9781112223334', 'Book C', 'Publisher C', 2020, '1st', 35.00, 10),
    ]

    orders_data = [
        (1, 1, '2024-11-01 10:00:00', 150.00, 'Pending'),  # Valid value
        (2, 2, '2024-11-02 14:30:00', 200.00, 'Shipped'),  # Valid value
    ]

    order_details_data = [
        (1, 1, 1, 2, 30.00, 5.00),  # OrderID = 1 (Must exist in Orders)
        (2, 2, 2, 1, 20.00, 0.00),  # OrderID = 2 (Must exist in Orders)
    ]

    authors_data = [
        (1, 'Author A'),
        (2, 'Author B'),
        (3, 'Author C'),
    ]

    book_authors_data = [
        (1, 1),
        (2, 2),
        (3, 3),
    ]

    genres_data = [
        (1, 'Fiction'),
        (2, 'Non-Fiction'),
        (3, 'Science'),
    ]

    book_genres_data = [
        (1, 1),
        (2, 2),
        (3, 3),
    ]

    notifications_data = [
        (1, 1, 'Welcome to our bookstore!', '2024-12-01 08:00:00', 'SystemAlert'),
        (2, 2, 'Your order has been shipped!', '2024-12-02 14:30:00', 'OrderUpdate'),
        (3, 3, 'Your order has been delivered.', '2024-12-03 16:45:00', 'OrderUpdate'),
        (4, 1, 'Special discount for you!', '2024-12-04 09:15:00', 'Promotions'),
    ]

    auditlogs_data = [
        (1, 'User logged in', '2024-12-01 08:00:00', 'User successfully logged in after entering correct credentials'),
        (2, 'Order placed', '2024-12-02 14:30:00', 'User placed an order for book "Python Programming"'),
        (3, 'Order shipped', '2024-12-03 16:45:00', 'Order #12345 has been shipped'),
        (1, 'System Alert', '2024-12-04 09:15:00',
         'System generated alert regarding low stock of book "Data Science 101"'),
    ]
    user_accounts_data = [
        (1, 'john_doe', 'hashedpassword123', 'Active', '2024-12-03 12:00:00'),
        (2, 'jane_doe', 'hashedpassword456', 'Suspended', None)
    ]

    # Insert data into respective tables
    # Existing data insertion (already in your code)
    inserter.bulk_insert_data('Users', [
        'UserID', 'FirstName', 'LastName', 'Email', 'PhoneNumber', 'Address',
        'RegistrationDate', 'TotalSpent', 'PermissionType'
    ], users_data)

    inserter.bulk_insert_data('Books', [
        'BookID', 'ISBN', 'Title', 'Publisher', 'PublicationYear', 'Edition', 'Price', 'Stock'
    ], books_data)

    inserter.bulk_insert_data('Orders', [
        'OrderID', 'UserID', 'OrderDate', 'TotalAmount', 'Status'
    ], orders_data)

    inserter.bulk_insert_data('OrderDetails', [
        'OrderDetailID', 'OrderID', 'BookID', 'Quantity', 'Subtotal', 'Discount'
    ], order_details_data)

    inserter.bulk_insert_data('Authors', [
        'AuthorID', 'Name'
    ], authors_data)

    inserter.bulk_insert_data('BookAuthors', [
        'BookID', 'AuthorID'
    ], book_authors_data)

    inserter.bulk_insert_data('Genres', [
        'GenreID', 'GenreName'
    ], genres_data)

    inserter.bulk_insert_data('BookGenres', [
        'BookID', 'GenreID'
    ], book_genres_data)

    inserter.bulk_insert_data('Notifications', [
        'NotificationID', 'UserID', 'Message', 'NotificationDate', 'NotificationType'
    ], notifications_data)

    inserter.bulk_insert_data('AuditLogs', [
        'UserID', 'Action', 'Timestamp', 'Details'
    ], auditlogs_data)

    # Missing data insertion
    inserter.bulk_insert_data('UserAccounts', [
        'UserID', 'Username', 'Password', 'AccountStatus', 'LastLogin'
    ], user_accounts_data)

    vendors_data = [
        ('Penguin Books', 'John Smith', 'john@penguin.com', '1234567890', '123 Main St, New York, NY', 'Books'),
        ('E-Book World', 'Jane Doe', 'jane@ebookworld.com', '9876543210', '456 Elm St, Los Angeles, CA', 'E-Books'),
        ('LogisticsPro', None, 'info@logisticspro.com', None, '789 Pine St, Chicago, IL', 'Logistics'),
    ]
    inserter.bulk_insert_data('Vendors', [
        'CompanyName', 'ContactName', 'ContactEmail', 'ContactPhone', 'Address', 'SupplyType'
    ], vendors_data)

    vendor_orders_data = [
        (1, 101, '2024-12-01', 50, 500.00, '2024-12-10', 'Pending'),
        (2, 102, '2024-11-25', 30, 300.00, '2024-12-05', 'Received'),
        (3, 103, None, 20, 200.00, None, 'Canceled'),
    ]
    inserter.bulk_insert_data('VendorOrders', [
        'VendorID', 'BookID', 'OrderDate', 'Quantity', 'TotalCost', 'DeliveryDate', 'OrderStatus'
    ], vendor_orders_data)


    # Commit changes and close connection
    inserter.commit_changes()
    inserter.disconnect()
