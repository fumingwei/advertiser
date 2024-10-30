SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for cu_group_member_relationship
-- ----------------------------
DROP TABLE IF EXISTS `cu_group_member_relationship`;
CREATE TABLE `cu_group_member_relationship`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `project_group_id` int NOT NULL COMMENT '项目组id',
  `user_id` int NOT NULL COMMENT '子账号id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '项目组和成员关系表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_project_groups
-- ----------------------------
DROP TABLE IF EXISTS `cu_project_groups`;
CREATE TABLE `cu_project_groups`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `project_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT '' COMMENT '项目组名称',
  `operation_type` json NULL COMMENT '操作类型',
  `mediums` json NULL COMMENT '投放媒介',
  `remark` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT '' COMMENT '备注',
  `company_id` int NULL DEFAULT NULL COMMENT '创建人id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '项目组表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cu_group_account_relationship
-- ----------------------------
DROP TABLE IF EXISTS `cu_group_account_relationship`;
CREATE TABLE `cu_group_account_relationship`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `project_group_id` int NOT NULL COMMENT '项目组id',
  `account_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '广告账户id',
  `account_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '广告账户名称',
  `medium` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '投放媒介',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

CREATE INDEX idx_account_id ON cu_group_account_relationship(account_id);
CREATE INDEX idx_group_id ON cu_group_account_relationship(project_group_id);

SET FOREIGN_KEY_CHECKS = 1;


CREATE TABLE `cu_files` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'id',
  `created_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `is_delete` tinyint(1) NOT NULL COMMENT '逻辑删除',
  `file_key` varchar(200) COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件key',
  `file_name` varchar(100) COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件名',
  `file_type` varchar(10) COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件类型',
  `file_size` varchar(10) COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件大小',
  `file_status` enum('PROCESS','SUCCEED','FAIL') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'PROCESS' COMMENT '文件状态',
  `description` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '文件描述',
  `expire_time` datetime DEFAULT NULL COMMENT '到期时间',
  `upload_user_id` int NOT NULL COMMENT '文件上传用户id',
  `download_status` tinyint(1) NOT NULL DEFAULT '0' COMMENT '下载状态',
  `remark` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;