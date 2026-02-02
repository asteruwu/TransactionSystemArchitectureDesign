CREATE TABLE IF NOT EXISTS `failed_orders` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `order_id` varchar(64) DEFAULT NULL,
  `original_json` text NOT NULL,
  `error_reason` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_order_id` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
