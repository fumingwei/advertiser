-- 删除线上已经无用的表
DROP TABLE IF EXISTS gatherone_oms.tb_pixel_propertys;
DROP TABLE IF EXISTS gatherone_oms.tb_account_renames;
DROP TABLE IF EXISTS gatherone_oms.tb_balance_transfers;
DROP TABLE IF EXISTS gatherone_oms.tb_balance_transfer_details;
DROP TABLE IF EXISTS gatherone_oms.tb_menus;
DROP TABLE IF EXISTS gatherone_oms.tb_actions;
DROP TABLE IF EXISTS gatherone_oms.tb_companies;
DROP TABLE IF EXISTS gatherone_oms.tb_departments;
DROP TABLE IF EXISTS gatherone_oms.tb_operate_logs;
DROP TABLE IF EXISTS gatherone_oms.tb_open_account_applications;
DROP TABLE IF EXISTS gatherone_oms.tb_permissions;

--修改表名
RENAME TABLE gatherone_oms.tb_advertiser_registers TO gatherone_oms.cu_advertiser_registers;
RENAME TABLE gatherone_oms.tb_advertiser_users TO gatherone_oms.cu_advertiser_users;
RENAME TABLE gatherone_oms.tb_user_cus_relationship TO gatherone_oms.cu_user_cus_relationship;
RENAME TABLE gatherone_oms.tb_work_orders TO gatherone_oms.cu_work_orders;


--v0.0.4版本之后新添加的表
/*
cu_account_renames
cu_balance_transfer_details
cu_balance_transfer_requests
cu_balance_transfers
cu_files
cu_login_histories
cu_meta_bm_account_details
cu_meta_bm_accounts
cu_meta_pixel_accounts
cu_meta_pixel_accounts_detail
cu_modules
cu_oe_open_accounts
cu_tiktok_bc_accounts
cu_tiktok_bc_accounts_details
cu_user_feedback
cu_operate_logs
cu_permissions
*/
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for cu_account_renames
-- ----------------------------
DROP TABLE IF EXISTS `cu_account_renames`;
CREATE TABLE `cu_account_renames`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `account_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户ID',
  `medium` enum('Meta','Tiktok','Kwai','Twitter','Apple','Petal','Google','X') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '媒体类型',
  `before_account_name` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户更改前名称',
  `after_account_name` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户更改后名称',
  `operate_result` enum('0','1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '0' COMMENT '操作结果',
  `operate_time` datetime NOT NULL COMMENT '操作时间',
  `remark` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `user_id` int NOT NULL COMMENT '提交人id',
  `request_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '请求编号',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_balance_transfer_details
-- ----------------------------
DROP TABLE IF EXISTS `cu_balance_transfer_details`;
CREATE TABLE `cu_balance_transfer_details`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `balance_transfer_id` int NOT NULL COMMENT '余额转移工单主键',
  `balance_transfer_request_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '余额转移主键',
  `account_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户',
  `medium` enum('Meta','Tiktok','Kwai','Twitter','Apple','Petal','Google') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户媒介',
  `bc_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT 'BC_ID',
  `before_balance` decimal(10, 2) NOT NULL COMMENT '交易前账户余额',
  `after_balance` decimal(10, 2) NOT NULL COMMENT '转出后账户余额',
  `amount` decimal(10, 2) NOT NULL COMMENT '交易金额',
  `trade_type` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '交易类型',
  `trade_result` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户充值结果结果',
  `remark` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `order_num` int NOT NULL COMMENT '排序字段',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_cu_balance_transfer_details_trade_type`(`trade_type`) USING BTREE,
  INDEX `ix_cu_balance_transfer_details_medium`(`medium`) USING BTREE,
  INDEX `ix_cu_balance_transfer_details_balance_transfer_id`(`balance_transfer_id`) USING BTREE,
  INDEX `ix_cu_balance_transfer_details_account_id`(`account_id`) USING BTREE,
  INDEX `ix_cu_balance_transfer_details_balance_transfer_request_id`(`balance_transfer_request_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_balance_transfer_requests
-- ----------------------------
DROP TABLE IF EXISTS `cu_balance_transfer_requests`;
CREATE TABLE `cu_balance_transfer_requests`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `balance_transfer_id` int NOT NULL COMMENT '余额转移工单主键',
  `internal_request_status` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '请求调用状态',
  `external_request_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '外部请求ID',
  `external_request_status` enum('Empty','Received','Running','Finished') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '接口返回的请求状态',
  `actual_amount` decimal(10, 2) NOT NULL COMMENT '实际交易金额',
  `trade_type` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '交易类型',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_cu_balance_transfer_requests_balance_transfer_id`(`balance_transfer_id`) USING BTREE,
  INDEX `ix_cu_balance_transfer_requests_trade_type`(`trade_type`) USING BTREE,
  INDEX `ix_cu_balance_transfer_requests_internal_request_status`(`internal_request_status`) USING BTREE,
  INDEX `ix_cu_balance_transfer_requests_external_request_status`(`external_request_status`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_balance_transfers
-- ----------------------------
DROP TABLE IF EXISTS `cu_balance_transfers`;
CREATE TABLE `cu_balance_transfers`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `customer_id` int NOT NULL COMMENT '客户主键',
  `user_id` int NOT NULL COMMENT '申请人ID',
  `transfer_status` enum('进行中','全部成功','全部失败','部分成功') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '转移状态',
  `transfer_amount` decimal(10, 2) NOT NULL COMMENT '转移总金额',
  `remark` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_cu_balance_transfers_transfer_status`(`transfer_status`) USING BTREE,
  INDEX `ix_cu_balance_transfers_user_id`(`user_id`) USING BTREE,
  INDEX `ix_cu_balance_transfers_customer_id`(`customer_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_files
-- ----------------------------
DROP TABLE IF EXISTS `cu_files`;
CREATE TABLE `cu_files`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `file_key` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件key',
  `file_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件名',
  `file_type` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件类型',
  `upload_user_id` int NOT NULL COMMENT '文件上传用户id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_login_histories
-- ----------------------------
DROP TABLE IF EXISTS `cu_login_histories`;
CREATE TABLE `cu_login_histories`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `user_id` int NULL DEFAULT NULL COMMENT '用户id',
  `device` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作系统',
  `browser` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '浏览器',
  `ip` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT 'ip地址',
  `country` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '国家',
  `request_status` enum('Success','Error') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '登录状态',
  `login_info` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '登录信息',
  `mobile` varchar(11) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '手机号',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_meta_bm_account_details
-- ----------------------------
DROP TABLE IF EXISTS `cu_meta_bm_account_details`;
CREATE TABLE `cu_meta_bm_account_details`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `account_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户ID',
  `operate_result` enum('0','1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '0' COMMENT '操作结果',
  `operate_time` datetime NULL DEFAULT NULL COMMENT '操作时间',
  `remark` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `bm_account_id` int NOT NULL COMMENT 'bm账户主表id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_meta_bm_accounts
-- ----------------------------
DROP TABLE IF EXISTS `cu_meta_bm_accounts`;
CREATE TABLE `cu_meta_bm_accounts`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `request_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '请求编号',
  `business_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '商业账户ID',
  `grant_type` enum('1','2','3') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '授权类型',
  `operate_type` enum('1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作类型',
  `operate_result` enum('DEFAULT','PART','ALL_SUCCEED','ALL_FAIL') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'DEFAULT' COMMENT '操作结果',
  `operate_time` datetime NOT NULL COMMENT '操作时间',
  `user_id` int NOT NULL COMMENT '提交人id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_meta_pixel_accounts
-- ----------------------------
DROP TABLE IF EXISTS `cu_meta_pixel_accounts`;
CREATE TABLE `cu_meta_pixel_accounts`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `request_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '请求编号',
  `pixel_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT 'Pixel_ID',
  `operate_type` enum('1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作类型',
  `operate_result` enum('DEFAULT','PART','ALL_SUCCEED','ALL_FAIL') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'DEFAULT' COMMENT '操作结果',
  `binding_time` datetime NOT NULL COMMENT '绑定时间',
  `user_id` int NOT NULL COMMENT '提交人id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_meta_pixel_accounts_detail
-- ----------------------------
DROP TABLE IF EXISTS `cu_meta_pixel_accounts_detail`;
CREATE TABLE `cu_meta_pixel_accounts_detail`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `account_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户_ID',
  `operate_result` enum('0','1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '0' COMMENT '操作结果',
  `binding_time` datetime NULL DEFAULT NULL COMMENT '绑定时间',
  `remark` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `pixel_account_id` int NOT NULL COMMENT 'Pixel,账户主表id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_modules
-- ----------------------------
DROP TABLE IF EXISTS `cu_modules`;
CREATE TABLE `cu_modules`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `module_code` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '模块代码',
  `module_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '模块名称',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_oe_open_accounts
-- ----------------------------
DROP TABLE IF EXISTS `cu_oe_open_accounts`;
CREATE TABLE `cu_oe_open_accounts`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `ticket_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '工单号',
  `ad_account_creation_request_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户创建请求ID',
  `ad_account_creation_request_status` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户创建状态',
  `customer_id` int NOT NULL COMMENT '结算名称ID',
  `oe_number` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '0E开户参考编号',
  `chinese_legal_entity_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '开户营业执照名称',
  `customer_type` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '客户类型',
  `business_registration` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '开户营业执照链接',
  `main_industry` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '主行业',
  `sub_industry` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '子行业',
  `org_ad_account_count` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户拥有数',
  `ad_account_limit` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户允许数量上限',
  `english_business_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '法律实体英文名称',
  `ad_accounts` json NOT NULL COMMENT '广告账户',
  `promotable_pages` json NOT NULL COMMENT '公共主页',
  `promotable_app_ids` json NOT NULL COMMENT '应用编号',
  `promotion_website` varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '推广网站',
  `approval_status` enum('PENDING','APPROVED','DISAPPROVED','CHANGES_REQUESTED') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'PENDING' COMMENT '审批状态',
  `approval_time` datetime NULL DEFAULT NULL COMMENT '审批时间',
  `remark` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `operate_asset_account_status` tinyint(1) NOT NULL COMMENT '销售资产组账户绑定状态',
  `operate_asset_account_id` json NULL COMMENT '销售资产组账户绑定主键数组',
  `user_id` int NOT NULL COMMENT '提交人ID',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_tiktok_bc_accounts
-- ----------------------------
DROP TABLE IF EXISTS `cu_tiktok_bc_accounts`;
CREATE TABLE `cu_tiktok_bc_accounts`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `request_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '请求编号',
  `business_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '商业账户ID',
  `cooperative_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '合作伙伴id',
  `grant_type` enum('1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '授权类型',
  `operate_type` enum('1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作类型',
  `operate_result` enum('DEFAULT','PART','ALL_SUCCEED','ALL_FAIL') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'DEFAULT' COMMENT '操作结果',
  `operate_time` datetime NOT NULL COMMENT '操作时间',
  `user_id` int NOT NULL COMMENT '提交人id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_tiktok_bc_accounts_details
-- ----------------------------
DROP TABLE IF EXISTS `cu_tiktok_bc_accounts_details`;
CREATE TABLE `cu_tiktok_bc_accounts_details`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `account_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户ID',
  `operate_result` enum('0','1','2') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '0' COMMENT '操作结果',
  `operate_time` datetime NULL DEFAULT NULL COMMENT '操作时间',
  `remark` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `tiktok_bc_account_id` int NOT NULL COMMENT 'bc账户主表id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_user_feedback
-- ----------------------------
DROP TABLE IF EXISTS `cu_user_feedback`;
CREATE TABLE `cu_user_feedback`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `content` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '反馈内容',
  `user_id` int NOT NULL COMMENT '用户id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_operate_logs
-- ----------------------------
DROP TABLE IF EXISTS `cu_operate_logs`;
CREATE TABLE `cu_operate_logs`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `module` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '系统模块',
  `request_path` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '请求地址',
  `request_user_id` int NULL DEFAULT NULL COMMENT '操作人员',
  `request_ip` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作IP',
  `request_address` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作地址',
  `request_status` enum('Success','Error') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作状态',
  `spent_time` int NOT NULL COMMENT '消耗时间(毫秒)',
  `session_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '会话编号',
  `request_method` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '请求方式',
  `required_params` json NOT NULL COMMENT '请求参数',
  `return_params` json NOT NULL COMMENT '返回参数',
  `operation` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作方法',
  `operation_desc` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作描述',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_permissions
-- ----------------------------
DROP TABLE IF EXISTS `cu_permissions`;
CREATE TABLE `cu_permissions`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `permission_code` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '权限代码',
  `permission_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '权限名称',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;