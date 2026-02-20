-- Test (test.ef.uk.com) and Prod (prod.ef.uk.com) databases only
CREATE DATABASE IF NOT EXISTS analytics_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS analytics_prod CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON analytics_test.* TO 'analytics'@'%';
GRANT ALL PRIVILEGES ON analytics_prod.* TO 'analytics'@'%';

FLUSH PRIVILEGES;
