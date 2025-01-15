<?php

namespace Mickeywaugh\Minio;

use Mickeywaugh\Minio\lib\Request;

class Minio
{
    const CODE_SUCCESS = 200;
    const CODE_DEL_SUCCESS = 204;
    private $accessKey;
    private $secretKey;
    private $endpoint;
    private $bucket;
    private $domain;
    private $multiCurl;
    private $curlOpts = [
        CURLOPT_CONNECTTIMEOUT => 30,
        CURLOPT_LOW_SPEED_LIMIT => 1,
        CURLOPT_LOW_SPEED_TIME => 30
    ];
    private static $instance;

    public function __construct(array $_config = [])
    {

        if (empty($_config)) {
            throw new \Exception('请配置Minio参数！');
        }

        $this->accessKey = $_config['accessKey'];
        $this->secretKey = $_config['secretKey'];
        $this->endpoint = $_config['endpoint'];
        $this->bucket = $_config['bucket'];
        $this->domain = $_config['domain'];

        if (empty($this->bucket)) $this->bucket = 'default';
        $this->multiCurl = curl_multi_init();
    }

    public function __destruct()
    {
        curl_multi_close($this->multiCurl);
    }

    /**
     * 单例模式 获取实例
     * @return Minio
     */
    public static function getInstance(): self
    {
        if (self::$instance == null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    /**
     * 设置当前桶
     */
    public function setBucket($bucket): self
    {
        $this->bucket = $bucket;
        return $this;
    }

    /**
     * 获取bucket列表
     * @param boolean $with_headers 是否返回header信息
     */
    public function listBuckets($with_headers = false): array
    {
        $res = $this->requestBucket('GET', '');
        if ($res['code'] == self::CODE_SUCCESS) {
            if (isset($res['data']['Buckets']['Bucket']['Name'])) { // 只有一个bucket的情况下
                $_buckets = [$res['data']['Buckets']['Bucket']['Name']];
            } else { // 多个 bucket
                $_buckets = array_column($res['data']['Buckets']['Bucket'], 'Name');
            }
            $res['data'] = ['Buckets' => $_buckets];
            return $this->success('获取成功！', $with_headers ? $res : $res['data']);
        } else {
            return $this->error($res['data']['Message'], $res['code'], $res['data']);
        }
    }

    /**
     * 获取bucket目录文件信息
     * @param string $bucket 桶名称
     * @param boolean $with_headers 是否返回header信息
     */
    public function getBucket(string $bucket, $with_headers = false): array
    {
        $res = $this->requestBucket('GET', $bucket);
        if ($res['code'] == self::CODE_SUCCESS) {
            if (isset($res['data']['Contents']['Key'])) $res['data']['Contents'] = [$res['data']['Contents']]; // 单个文件
            return $this->success('获取成功！', $with_headers ? $res : $res['data']);
        } else {
            return $this->error($res['data']['Message'], $res['code'], $res['data']);
        }
    }

    /**
     * 创建bucket目录
     * @param string $bucket 桶名称
     */
    public function createBucket(string $bucket): bool
    {
        $res = $this->requestBucket('PUT', $bucket);
        return $res['code'] == self::CODE_SUCCESS;
    }

    /**
     * 删除bucket目录
     * @param string $bucket 桶名称
     */
    public function deleteBucket(string $bucket): bool
    {
        $res = $this->requestBucket('DELETE', $bucket);
        return $res['code'] == self::CODE_SUCCESS;
    }

    /**
     * 上传文件
     * @param string $file 本地需要上传的绝对路径文件
     * @param string $uri 保存路径名称，不包含桶名称
     */
    public function putObject(string $file, string $uri, $with_headers = false): array
    {
        // 判断bucket是否存在，不存在则创建
        $rel = $this->listBuckets();
        if ($rel['status']) return $rel;
        if (!in_array($this->bucket, $rel['data']['Buckets'])) {
            $this->createBucket($this->bucket);
        }

        // 发送请求
        $request = (new Request('PUT', $this->endpoint, $this->getObjectUri($uri)))
            ->setFileContents(fopen($file, 'r'))
            ->setMultiCurl($this->multiCurl)
            ->setCurlOpts($this->curlOpts)
            ->sign($this->accessKey, $this->secretKey);

        $res = $this->objectToArray($request->getResponse());

        if ($res['code'] == self::CODE_SUCCESS) {
            return $this->success('上传成功！', $with_headers ? $res : $res['data']);
        } else {
            return $this->error($res['data']['Message'], $res['code'], $res['data']);
        }
    }

    /**
     * 获取文件链接
     * @param string $uri 保存路径名称
     */
    public function getObjectUrl(string $uri): string
    {
        return trim($this->domain, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $this->getObjectUri($uri);
    }

    /**
     * 获取文件地址
     * @param string $uri 保存路径名称
     */
    public function getObjectUri(string $uri): string
    {
        return trim($this->bucket, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . trim($uri, DIRECTORY_SEPARATOR);
    }

    /**
     * 获取文件类型，header中体现
     * @param string $uri 保存路径名称
     */
    public function getObjectInfo(string $uri): array
    {
        $request = (new Request('HEAD', $this->endpoint, $this->getObjectUri($uri)))
            ->setMultiCurl($this->multiCurl)
            ->setCurlOpts($this->curlOpts)
            ->sign($this->accessKey, $this->secretKey);

        $res = $this->objectToArray($request->getResponse());

        if ($res['code'] == self::CODE_SUCCESS) {
            return $this->success('获取成功！', $res['headers']);
        } else {
            return $this->error($res['data']['Message'], $res['code'], $res['data']);
        }
    }

    /**
     * 获取文件 ，data返回二进制数据流
     * @param string $uri 保存路径名称
     * @param boolean $with_headers 是否返回header信息
     */
    public function getObject(string $uri, $with_headers = false): array
    {
        $request = (new Request('GET', $this->endpoint, $this->getObjectUri($uri)))
            ->setMultiCurl($this->multiCurl)
            ->setCurlOpts($this->curlOpts)
            ->sign($this->accessKey, $this->secretKey);

        $res = $this->objectToArray($request->getResponse());

        if ($res['code'] == self::CODE_SUCCESS) {
            return $this->success('获取成功！', $with_headers ? $res : $res['data']);
        } else {
            return $this->error($res['data']['Message'], $res['code'], $res['data']);
        }
    }

    /**
     * 删除文件
     * @param string $uri 保存路径名称
     */
    public function deleteObject(string $uri): bool
    {
        $request = (new Request('DELETE', $this->endpoint, $this->getObjectUri($uri)))
            ->setMultiCurl($this->multiCurl)
            ->setCurlOpts($this->curlOpts)
            ->sign($this->accessKey, $this->secretKey);

        $res = $this->objectToArray($request->getResponse());
        return $res['code'] == self::CODE_DEL_SUCCESS;
    }

    /**
     * 复制文件
     * @param string $fromObject 源文件
     * @param string $toObject 目标文件
     */
    public function copyObject(string $fromObject, string $toObject): bool
    {
        $request = (new Request('PUT', $this->endpoint, $this->getObjectUri($toObject)))
            ->setHeaders(['x-amz-copy-source' => $this->getObjectUri($fromObject)])
            ->setMultiCurl($this->multiCurl)
            ->setCurlOpts($this->curlOpts)
            ->sign($this->accessKey, $this->secretKey);

        $res = $this->objectToArray($request->getResponse());

        return $res['code'] == self::CODE_SUCCESS;
    }

    /**
     * 移动文件
     * @param string $fromObject 源文件
     * @param string $toObject 目标文件
     * */
    public function moveObject(string $fromObject, string $toObject): bool
    {
        // 复制文件
        $res = $this->copyObject($fromObject, $toObject);
        if ($res) {
            // 删除源文件
            $res2 = $this->deleteObject($fromObject);
        }
        return $res && $res2;
    }

    /**
     * bucket目录请求
     * @param string $method
     * @param string $bucket
     * @param array $headers
     * @return mixed
     */
    protected function requestBucket(string $method = 'GET', string $bucket = ''): array
    {
        $request = (new Request($method, $this->endpoint, $bucket))
            ->setMultiCurl($this->multiCurl)
            ->setCurlOpts($this->curlOpts)
            ->sign($this->accessKey, $this->secretKey);

        return $this->objectToArray($request->getResponse());
    }

    /**
     * 对象转数组
     * @param $object
     * @return mixed
     */
    private function objectToArray($object): array
    {
        $arr = is_object($object) ? get_object_vars($object) : $object;
        $returnArr = [];
        foreach ($arr as $key => $val) {
            $val = (is_array($val)) || is_object($val) ? $this->objectToArray($val) : $val;
            $returnArr[$key] = $val;
        }
        return $returnArr;
    }

    private function success(string $msg = '操作成功', array $data = []): array
    {
        return ['status' => 0, 'msg' => $msg, 'data' => $data];
    }

    private function error(string $msg = '出错了', int $status = 1, array $data = []): array
    {
        return ['status' => $status, 'msg' => $msg, 'data' => $data];
    }
}
