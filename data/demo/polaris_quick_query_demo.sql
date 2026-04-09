-- Demo database for NL2SQL Agent quick-query cards
-- Target: MariaDB / MySQL 8+
-- Import:
--   mysql -uroot -p < data/demo/polaris_quick_query_demo.sql

DROP DATABASE IF EXISTS polaris_demo;
CREATE DATABASE polaris_demo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE polaris_demo;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS user_activity_log;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS regions;

SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE regions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    region_name VARCHAR(50) NOT NULL,
    region_manager VARCHAR(50) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE departments (
    id INT PRIMARY KEY AUTO_INCREMENT,
    department_name VARCHAR(50) NOT NULL,
    region_id INT NOT NULL,
    budget DECIMAL(12, 2) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_departments_region FOREIGN KEY (region_id) REFERENCES regions(id)
);

CREATE TABLE employees (
    id INT PRIMARY KEY AUTO_INCREMENT,
    employee_name VARCHAR(50) NOT NULL,
    department_id INT NOT NULL,
    title VARCHAR(50) NOT NULL,
    hire_date DATE NOT NULL,
    monthly_salary DECIMAL(10, 2) NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_employees_department FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(80) NOT NULL,
    region_id INT NOT NULL,
    email VARCHAR(120) NOT NULL,
    customer_level VARCHAR(20) NOT NULL DEFAULT 'standard',
    registered_at DATETIME NOT NULL,
    last_active_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customers_region FOREIGN KEY (region_id) REFERENCES regions(id)
);

CREATE TABLE categories (
    id INT PRIMARY KEY AUTO_INCREMENT,
    category_name VARCHAR(50) NOT NULL,
    parent_category VARCHAR(50) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100) NOT NULL,
    category_id INT NOT NULL,
    sku VARCHAR(40) NOT NULL UNIQUE,
    unit_price DECIMAL(10, 2) NOT NULL,
    cost_price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_products_category FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE inventory (
    id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    warehouse_name VARCHAR(50) NOT NULL,
    stock_quantity INT NOT NULL,
    reorder_level INT NOT NULL DEFAULT 20,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_inventory_product FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_no VARCHAR(30) NOT NULL UNIQUE,
    customer_id INT NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    payment_status VARCHAR(20) NOT NULL DEFAULT 'paid',
    order_date DATETIME NOT NULL,
    shipped_at DATETIME NULL,
    completed_at DATETIME NULL,
    total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    line_amount DECIMAL(12, 2) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(id),
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE user_activity_log (
    id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    activity_at DATETIME NOT NULL,
    source_channel VARCHAR(30) NOT NULL,
    metadata_json JSON NULL,
    CONSTRAINT fk_activity_customer FOREIGN KEY (customer_id) REFERENCES customers(id)
);

INSERT INTO regions (region_name, region_manager) VALUES
('华北', '王强'),
('华东', '李敏'),
('华南', '周婷'),
('西南', '陈峰');

INSERT INTO departments (department_name, region_id, budget) VALUES
('销售一部', 1, 500000),
('销售二部', 2, 650000),
('电商运营', 2, 420000),
('客户成功', 3, 360000),
('供应链', 4, 300000),
('财务', 1, 280000);

INSERT INTO employees (employee_name, department_id, title, hire_date, monthly_salary) VALUES
('张蕾', 1, '销售经理', '2022-03-10', 18000),
('赵阳', 1, '销售专员', '2023-01-08', 12000),
('何静', 1, '销售专员', '2023-06-18', 11800),
('吴晨', 2, '销售经理', '2021-11-03', 19000),
('刘帆', 2, '销售专员', '2024-02-15', 11000),
('顾宁', 3, '运营主管', '2022-08-20', 16000),
('冯雪', 3, '运营专员', '2023-07-02', 10500),
('唐悦', 4, '客户成功经理', '2022-05-12', 15000),
('韩涛', 4, '客户成功专员', '2024-01-05', 9800),
('孙捷', 5, '供应链主管', '2021-09-28', 17000),
('袁琳', 5, '采购专员', '2024-03-11', 9200),
('马骁', 6, '财务经理', '2020-12-01', 18500);

INSERT INTO customers (customer_name, region_id, email, customer_level, registered_at, last_active_at) VALUES
('星河科技', 1, 'xinghe@example.com', 'vip', NOW() - INTERVAL 180 DAY, NOW() - INTERVAL 1 DAY),
('晨曦零售', 2, 'chenxi@example.com', 'gold', NOW() - INTERVAL 150 DAY, NOW() - INTERVAL 2 DAY),
('海蓝商贸', 3, 'hailan@example.com', 'standard', NOW() - INTERVAL 120 DAY, NOW() - INTERVAL 5 DAY),
('远山食品', 4, 'yuanshan@example.com', 'standard', NOW() - INTERVAL 90 DAY, NOW() - INTERVAL 12 DAY),
('北辰医药', 1, 'beichen@example.com', 'gold', NOW() - INTERVAL 200 DAY, NOW() - INTERVAL 3 DAY),
('极光家居', 2, 'jiguang@example.com', 'vip', NOW() - INTERVAL 60 DAY, NOW() - INTERVAL 1 DAY),
('云途教育', 3, 'yuntu@example.com', 'standard', NOW() - INTERVAL 75 DAY, NOW() - INTERVAL 9 DAY),
('青禾农业', 4, 'qinghe@example.com', 'standard', NOW() - INTERVAL 110 DAY, NOW() - INTERVAL 20 DAY),
('华映传媒', 2, 'huaying@example.com', 'gold', NOW() - INTERVAL 95 DAY, NOW() - INTERVAL 4 DAY),
('凌云汽配', 1, 'lingyun@example.com', 'vip', NOW() - INTERVAL 130 DAY, NOW() - INTERVAL 6 DAY),
('未单客户A', 3, 'nevera@example.com', 'standard', NOW() - INTERVAL 45 DAY, NOW() - INTERVAL 8 DAY),
('未单客户B', 4, 'neverb@example.com', 'standard', NOW() - INTERVAL 35 DAY, NOW() - INTERVAL 14 DAY);

INSERT INTO categories (category_name, parent_category) VALUES
('办公设备', NULL),
('电子配件', NULL),
('家居用品', NULL),
('食品饮料', NULL),
('营销物料', NULL);

INSERT INTO products (product_name, category_id, sku, unit_price, cost_price, status) VALUES
('智能打印机 P1', 1, 'P-OFF-001', 1899.00, 1320.00, 'active'),
('商用扫描仪 S2', 1, 'P-OFF-002', 2599.00, 1860.00, 'active'),
('会议平板 M3', 1, 'P-OFF-003', 6999.00, 5200.00, 'active'),
('无线鼠标', 2, 'P-ACC-001', 89.00, 42.00, 'active'),
('机械键盘', 2, 'P-ACC-002', 299.00, 168.00, 'active'),
('扩展坞 Pro', 2, 'P-ACC-003', 499.00, 320.00, 'active'),
('人体工学椅', 3, 'P-HOM-001', 1299.00, 860.00, 'active'),
('升降桌', 3, 'P-HOM-002', 2399.00, 1600.00, 'active'),
('收纳柜', 3, 'P-HOM-003', 799.00, 510.00, 'active'),
('精品咖啡豆', 4, 'P-FOO-001', 129.00, 68.00, 'active'),
('气泡饮料礼盒', 4, 'P-FOO-002', 199.00, 105.00, 'active'),
('零食礼包', 4, 'P-FOO-003', 159.00, 88.00, 'active'),
('品牌手册', 5, 'P-MKT-001', 39.00, 12.00, 'active'),
('展会海报套装', 5, 'P-MKT-002', 149.00, 55.00, 'active'),
('定制礼品袋', 5, 'P-MKT-003', 19.90, 6.50, 'active');

INSERT INTO inventory (product_id, warehouse_name, stock_quantity, reorder_level) VALUES
(1, '北京仓', 22, 15),
(2, '上海仓', 8, 12),
(3, '广州仓', 6, 10),
(4, '北京仓', 120, 30),
(5, '上海仓', 48, 20),
(6, '广州仓', 15, 18),
(7, '成都仓', 11, 12),
(8, '上海仓', 7, 10),
(9, '北京仓', 28, 15),
(10, '广州仓', 96, 25),
(11, '成都仓', 18, 20),
(12, '上海仓', 13, 15),
(13, '北京仓', 300, 50),
(14, '广州仓', 9, 12),
(15, '成都仓', 14, 20);

INSERT INTO orders (order_no, customer_id, order_status, payment_status, order_date, shipped_at, completed_at, total_amount) VALUES
('SO202604001', 1, 'completed', 'paid', NOW() - INTERVAL 2 DAY, NOW() - INTERVAL 1 DAY, NOW() - INTERVAL 1 DAY, 4576.00),
('SO202604002', 2, 'completed', 'paid', NOW() - INTERVAL 5 DAY, NOW() - INTERVAL 4 DAY, NOW() - INTERVAL 3 DAY, 3197.00),
('SO202604003', 3, 'processing', 'paid', NOW() - INTERVAL 1 DAY, NULL, NULL, 1398.00),
('SO202604004', 4, 'pending', 'unpaid', NOW() - INTERVAL 3 DAY, NULL, NULL, 498.00),
('SO202604005', 5, 'completed', 'paid', NOW() - INTERVAL 10 DAY, NOW() - INTERVAL 9 DAY, NOW() - INTERVAL 8 DAY, 7897.00),
('SO202604006', 6, 'completed', 'paid', NOW() - INTERVAL 12 DAY, NOW() - INTERVAL 11 DAY, NOW() - INTERVAL 10 DAY, 2798.00),
('SO202604007', 1, 'completed', 'paid', NOW() - INTERVAL 18 DAY, NOW() - INTERVAL 17 DAY, NOW() - INTERVAL 16 DAY, 388.00),
('SO202604008', 7, 'cancelled', 'refunded', NOW() - INTERVAL 14 DAY, NULL, NULL, 799.00),
('SO202604009', 8, 'processing', 'paid', NOW() - INTERVAL 6 DAY, NULL, NULL, 2598.00),
('SO202604010', 9, 'completed', 'paid', NOW() - INTERVAL 20 DAY, NOW() - INTERVAL 19 DAY, NOW() - INTERVAL 18 DAY, 646.00),
('SO202603011', 10, 'completed', 'paid', DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 5 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 4 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 3 DAY), 5298.00),
('SO202603012', 2, 'completed', 'paid', DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 9 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 8 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 7 DAY), 1198.00),
('SO202603013', 5, 'completed', 'paid', DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 12 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 11 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 10 DAY), 2187.00),
('SO202603014', 6, 'completed', 'paid', DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 15 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 14 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 13 DAY), 278.80),
('SO202603015', 1, 'completed', 'paid', DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 20 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 19 DAY), DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 18 DAY), 9598.00);

INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_amount) VALUES
(1, 1, 2, 1899.00, 3798.00),
(1, 4, 4, 89.00, 356.00),
(1, 13, 10, 39.00, 390.00),
(1, 15, 16, 19.90, 318.40),
(2, 2, 1, 2599.00, 2599.00),
(2, 10, 2, 129.00, 258.00),
(2, 12, 2, 159.00, 318.00),
(3, 7, 1, 1299.00, 1299.00),
(3, 4, 1, 89.00, 89.00),
(3, 13, 5, 39.00, 195.00),
(4, 11, 1, 199.00, 199.00),
(4, 14, 2, 149.00, 298.00),
(5, 3, 1, 6999.00, 6999.00),
(5, 4, 2, 89.00, 178.00),
(5, 10, 4, 129.00, 516.00),
(5, 13, 4, 39.00, 156.00),
(5, 15, 4, 19.90, 79.60),
(6, 8, 1, 2399.00, 2399.00),
(6, 15, 20, 19.90, 398.00),
(7, 11, 1, 199.00, 199.00),
(7, 12, 1, 159.00, 159.00),
(7, 15, 1, 19.90, 19.90),
(8, 9, 1, 799.00, 799.00),
(9, 2, 1, 2599.00, 2599.00),
(10, 5, 1, 299.00, 299.00),
(10, 10, 1, 129.00, 129.00),
(10, 13, 2, 39.00, 78.00),
(10, 15, 7, 19.90, 139.30),
(11, 8, 2, 2399.00, 4798.00),
(11, 10, 2, 129.00, 258.00),
(11, 15, 12, 19.90, 238.80),
(12, 7, 1, 1299.00, 1299.00),
(12, 13, 3, 39.00, 117.00),
(12, 15, 4, 19.90, 79.60),
(13, 1, 1, 1899.00, 1899.00),
(13, 10, 2, 129.00, 258.00),
(13, 12, 1, 159.00, 159.00),
(13, 13, 2, 39.00, 78.00),
(13, 15, 2, 19.90, 39.80),
(14, 15, 14, 19.90, 278.60),
(15, 3, 1, 6999.00, 6999.00),
(15, 8, 1, 2399.00, 2399.00),
(15, 13, 5, 39.00, 195.00),
(15, 15, 5, 19.90, 99.50);

INSERT INTO user_activity_log (customer_id, activity_type, activity_at, source_channel, metadata_json) VALUES
(1, 'login', NOW() - INTERVAL 1 DAY, 'web', JSON_OBJECT('device', 'desktop')),
(1, 'order_view', NOW() - INTERVAL 2 DAY, 'web', JSON_OBJECT('page', 'orders')),
(2, 'login', NOW() - INTERVAL 2 DAY, 'mobile', JSON_OBJECT('device', 'ios')),
(2, 'cart_update', NOW() - INTERVAL 4 DAY, 'web', JSON_OBJECT('cart_items', 3)),
(3, 'login', NOW() - INTERVAL 5 DAY, 'web', JSON_OBJECT('device', 'desktop')),
(5, 'login', NOW() - INTERVAL 3 DAY, 'mobile', JSON_OBJECT('device', 'android')),
(6, 'promotion_click', NOW() - INTERVAL 1 DAY, 'email', JSON_OBJECT('campaign', 'spring_sale')),
(7, 'support_ticket', NOW() - INTERVAL 6 DAY, 'web', JSON_OBJECT('priority', 'medium')),
(9, 'login', NOW() - INTERVAL 4 DAY, 'web', JSON_OBJECT('device', 'desktop')),
(10, 'wishlist_update', NOW() - INTERVAL 7 DAY, 'mobile', JSON_OBJECT('items', 2)),
(11, 'login', NOW() - INTERVAL 8 DAY, 'web', JSON_OBJECT('device', 'desktop')),
(12, 'profile_edit', NOW() - INTERVAL 10 DAY, 'web', JSON_OBJECT('field', 'email'));

CREATE OR REPLACE VIEW v_order_details AS
SELECT
    o.order_no,
    o.order_status,
    o.order_date,
    c.customer_name,
    r.region_name,
    p.product_name,
    cat.category_name,
    oi.quantity,
    oi.unit_price,
    oi.line_amount
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN regions r ON r.id = c.region_id
JOIN order_items oi ON oi.order_id = o.id
JOIN products p ON p.id = oi.product_id
JOIN categories cat ON cat.id = p.category_id;

-- Suggested NL questions supported by this schema:
-- 1. 统计每个部门的员工数量
-- 2. 计算过去30天的订单总金额
-- 3. 查询销售额排名前10的产品
-- 4. 分析各地区的销售趋势
-- 5. 找出复购率最高的客户
-- 6. 对比本月与上月的销售业绩
-- 7. 查询最近一周的活跃用户
-- 8. 列出所有未完成的订单
-- 9. 显示库存不足的商品
-- 10. 查询每个订单的详细信息包括客户和产品
-- 11. 找出从未下单的客户
-- 12. 统计每个分类下的产品数量和平均价格
