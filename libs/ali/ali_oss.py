import oss2
from settings.base import configs

ACCESS_KEY_ID = configs.ACCESSKEY_ID
ACCESS_KEY_SECRET = configs.ACCESSKEY_SECRET
BUCKET_NAME = configs.BUCKET_NAME
END_POINT = configs.END_POINT


class OssManage:
    def __init__(self):
        self.AccessKeyId = ACCESS_KEY_ID
        self.AccessKeySecret = ACCESS_KEY_SECRET
        self.Endpoint = END_POINT
        self.BucketName = BUCKET_NAME
        try:
            self.auth = oss2.Auth(self.AccessKeyId, self.AccessKeySecret)
            self.bucket = oss2.Bucket(self.auth, self.Endpoint, self.BucketName)
            self.bucket.create_bucket(
                oss2.models.BUCKET_ACL_PUBLIC_READ
            )  # 设为公共读权限
        except Exception as e:
            raise e
        # bucket_info = self.bucket.get_bucket_info()

    # 删除文件
    def file_delete(self, key):
        try:
            self.bucket.delete_object(key)
        except Exception as e:
            raise e

    # 上传文件
    def file_upload(self, key, file):
        try:
            result = self.bucket.put_object(key, file)
            return result
        except Exception as e:
            raise e

    # 获取文件
    def get_obj(self, key):
        try:
            result = self.bucket.get_object(key)
            return result
        except oss2.exceptions.NoSuchKey as e:
            raise e

    # 获取bucket所有文件  返回文件对象生成器
    def get_all_obj(self):
        try:
            return oss2.ObjectIterator(self.bucket)
        except Exception as e:
            raise e

    # 监测文件是否已存在
    def exist_valid(self, key):
        try:
            exist = self.bucket.object_exists(key)
            if exist:
                return True, "File Existed"
            return False, "File Not Existed"
        except Exception as e:
            raise e

    def get_bucket(self):
        auth = oss2.Auth(self.AccessKeyId, self.AccessKeySecret)
        bucket = oss2.Bucket(auth, self.Endpoint, self.BucketName)
        return bucket
