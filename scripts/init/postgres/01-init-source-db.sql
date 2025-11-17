-- Инициализация source базы данных
CREATE DATABASE source_db;

\c source_db;

-- Таблица customers
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    country_code VARCHAR(2) DEFAULT 'US',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица orders
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица payments
CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    amount DECIMAL(10,2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для производительности
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_payments_order_id ON payments(order_id);

-- Триггер для обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Тестовые данные
INSERT INTO customers (name, email, country_code) VALUES 
('John Doe', 'john.doe@example.com', 'US'),
('Jane Smith', 'jane.smith@example.com', 'GB'),
('Bob Johnson', 'bob.johnson@example.com', 'CA'),
('Alice Brown', 'alice.brown@example.com', 'AU'),
('Carlos Silva', 'carlos.silva@example.com', 'BR'),
('Wei Zhang', 'wei.zhang@example.com', 'CN'),
('Hans Mueller', 'hans.mueller@example.com', 'DE');

INSERT INTO orders (customer_id, amount, status) VALUES 
(1, 100.50, 'completed'),
(2, 75.25, 'completed'),
(3, 200.00, 'pending'),
(1, 50.00, 'completed'),
(4, 150.75, 'completed');

INSERT INTO payments (order_id, amount, payment_method, status) VALUES
(1, 100.50, 'credit_card', 'completed'),
(2, 75.25, 'paypal', 'completed'),
(4, 50.00, 'credit_card', 'completed'),
(5, 150.75, 'bank_transfer', 'completed');

-- Публикация для Debezium
CREATE PUBLICATION dbz_publication FOR ALL TABLES;