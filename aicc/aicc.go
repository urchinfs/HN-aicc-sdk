package aicc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/urchinfs/HN-aicc-sdk/types"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/donnie4w/go-logger/logger"
	"github.com/go-resty/resty/v2"
	urchinutil "github.com/urchinfs/urchin_util/redis"
)

const (
	DefaultRootPath = "/share/org/TEST/pcl_test01"
)

type Client interface {
	StorageVolExists(ctx context.Context, storageVolName string) (bool, error)

	MakeStorageVol(ctx context.Context, storageVolName string) (err error)

	ListStorageVols(ctx context.Context) ([]StorageVolInfo, error)

	StatFile(ctx context.Context, storageVolName, filePath string) (FileInfo, error)

	UploadFile(ctx context.Context, storageVolName, filePath, digest string, totalLength int64, reader io.Reader) error

	DownloadFile(ctx context.Context, storageVolName, filePath string) (io.ReadCloser, error)

	RemoveFile(ctx context.Context, storageVolName, filePath string) error

	RemoveFiles(ctx context.Context, storageVolName string, objects []*FileInfo) error

	RemoveFolder(ctx context.Context, storageVolName string, folderName string) error

	ListFiles(ctx context.Context, storageVolName, prefix, marker string, limit int64) ([]*FileInfo, error)

	ListDirFiles(ctx context.Context, storageVolName, prefix string) ([]*FileInfo, error)

	IsFileExist(ctx context.Context, storageVolName, filePath string) (bool, error)

	IsStorageVolExist(ctx context.Context, storageVolName string) (bool, error)

	GetDownloadLink(ctx context.Context, storageVolName, filePath string, expire time.Duration) (string, error)

	CreateFolder(ctx context.Context, storageVolName, folderName string) error

	StatFolder(ctx context.Context, storageVolName, folderName string) (*FileInfo, bool, error)

	PostTransfer(ctx context.Context, storageVolName, filePath string, isSuccess bool) error
}

type client struct {
	httpClient     *resty.Client
	redisStorage   *urchinutil.RedisStorage
	token          string
	username       string
	password       string
	endpoint       string
	redisEndpoints []string
	redisPassword  string
	enableCluster  bool
}

func New(username, password, endpoint string, redisEndpoints []string, redisPassword string, enableCluster bool) (Client, error) {
	c := &client{
		username:       username,
		password:       password,
		endpoint:       endpoint,
		redisEndpoints: redisEndpoints,
		redisPassword:  redisPassword,
		enableCluster:  enableCluster,
		httpClient:     resty.New(),
		redisStorage:   urchinutil.NewRedisStorage(redisEndpoints, redisPassword, enableCluster),
	}

	c.endpoint = "http://" + c.endpoint
	if c.username == "" || c.password == "" || c.endpoint == "" {
		return nil, types.ErrorInvalidParameter
	}
	if c.redisStorage == nil {
		return nil, errors.New("init redis error")
	}

	return c, nil
}

type StorageVolInfo struct {
	Name string `json:"name"`
}

type FileInfo struct {
	Key          string
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
	Expires      time.Time
	Metadata     http.Header
}

type Reply struct {
	Code   int             `json:"code"`
	Msg    string          `json:"msg"`
	RetObj json.RawMessage `json:"retObj"`
}

func parseBody(ctx context.Context, reply *Reply, body interface{}) error {
	if body != nil {
		err := json.Unmarshal(reply.RetObj, body)
		if err != nil {
			logger.Errorf("parseBody json Unmarshal failed, error:%v", err)
			return types.ErrorJsonUnmarshalFailed
		}
	}

	return nil
}

func (c *client) completePath(storageVolName string, elem ...string) string {
	elems := append([]string{DefaultRootPath, storageVolName}, elem...)
	return path.Join(elems...)
}

func (c *client) needRetry(r *Reply) bool {
	if r.Code == 401 {
		tokenKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixAiCcToken)
		_ = c.redisStorage.Del(tokenKey)
		_ = c.refreshToken(context.Background())

		return true
	}

	return false
}

func (c *client) sendHttpRequest(ctx context.Context, httpMethod, httpPath string, jsonBody string, respData interface{}) error {
	if !strings.HasPrefix(httpPath, "/") {
		httpPath = "/" + httpPath
	}
	httpUrl := c.endpoint + httpPath

	for {
		r := &Reply{}
		response := &resty.Response{}
		var err error
		if httpMethod == types.HttpMethodGet {
			response, err = c.httpClient.R().
				SetHeader(types.AuthHeader, c.token).
				SetResult(r).
				Get(httpUrl)
			if err != nil {
				return err
			}
		} else if httpMethod == types.HttpMethodPost {
			response, err = c.httpClient.R().
				SetHeader("Content-Type", "application/json").
				SetHeader(types.AuthHeader, c.token).
				SetBody(jsonBody).SetResult(r).
				Post(httpUrl)
			if err != nil {
				return err
			}
		} else if httpMethod == types.HttpMethodDelete {
			response, err = c.httpClient.R().
				SetHeader(types.AuthHeader, c.token).
				SetResult(r).
				Delete(httpUrl)
			if err != nil {
				return err
			}
		} else {
			return types.ErrorInternal
		}

		if !response.IsSuccess() {
			if response.StatusCode() == http.StatusUnauthorized {
				err := c.delToken(ctx)
				if err != nil {
					return err
				}
				err = c.refreshToken(ctx)
				if err != nil {
					return err
				}

				time.Sleep(time.Second * 2)
				continue
			}

			r := &Reply{}
			err := json.Unmarshal(response.Body(), r)
			if err == nil {
				if c.needRetry(r) {
					time.Sleep(time.Second * 2)
					continue
				}
			}

			return errors.New("Code:" + strconv.FormatInt(int64(response.StatusCode()), 10) + ", Msg:" + string(response.Body()))
		}

		if r.Code != http.StatusOK {
			return fmt.Errorf("resp Code:%v, Err:%s", r.Code, r.Msg)
		}

		err = parseBody(ctx, r, respData)
		if err != nil {
			return err
		}

		break
	}

	return nil
}

func (c *client) getToken(ctx context.Context) (string, error) {
	type GetAuthRequest struct {
		UserName string `json:"username"`
		Password string `json:"password"`
	}

	req := GetAuthRequest{
		UserName: c.username,
		Password: c.password,
	}

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return "", types.ErrorJsonMarshalFailed
	}

	type GetTokenReply struct {
		Code   int32  `json:"code"`
		Msg    string `json:"msg"`
		RetObj struct {
			Token string `json:"token"`
		} `json:"retObj"`
	}

	urlPath := fmt.Sprintf("/appspace/v1/loginForToken")
	resp := &GetTokenReply{}
	response, err := c.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(jsonBody).SetResult(resp).Post(c.endpoint + urlPath)
	if err != nil {
		return "", err
	}

	if !response.IsSuccess() {
		return "", fmt.Errorf("http status Code:%v, Body:%s", response.StatusCode(), response.Body())
	}

	if resp.Code != http.StatusOK {
		return "", fmt.Errorf("authentication Failed, Code:%v, Msg%s", resp.Code, resp.Msg)
	}
	authToken := resp.RetObj.Token

	return authToken, nil
}

func (c *client) refreshToken(ctx context.Context) error {
	tokenKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixAiCcToken)
	value, err := c.redisStorage.Get(tokenKey)
	if err != nil || len(value) <= 0 {
		if errors.Is(err, types.ErrorNotExists) || len(value) <= 0 {
			token, err := c.getToken(ctx)
			if err != nil {
				return err
			}

			c.token = token
			err = c.redisStorage.SetWithTimeout(tokenKey, []byte(token), types.DefaultTokenExpireTime)
			if err != nil {
				return err
			}

			return nil
		}

		return err
	}

	c.token = string(value)
	return nil
}

func (c *client) delToken(ctx context.Context) error {
	tokenKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixAiCcToken)
	return c.redisStorage.Del(tokenKey)
}

func (c *client) StorageVolExists(ctx context.Context, storageVolName string) (bool, error) {
	vols, err := c.ListStorageVols(ctx)
	if err != nil {
		return false, err
	}

	for _, vol := range vols {
		if vol.Name == storageVolName {
			return true, nil
		}
	}

	return false, nil
}

func (c *client) MakeStorageVol(ctx context.Context, storageVolName string) (err error) {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	type MakeStorageVolReq struct {
		FilePath string `json:"filePath"`
	}

	req := &MakeStorageVolReq{
		FilePath: c.completePath(storageVolName),
	}

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return types.ErrorJsonMarshalFailed
	}

	reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/createUserJobFileDir")
	err = c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonBody), nil)
	if err != nil {
		return err
	}

	return nil
}

type FileListReq struct {
	FilePath string `json:"filePath"`
}

type FileListReply struct {
	FileName string `json:"fileName"`
	FileType string `json:"fileType"`
	FileSize int64  `json:"fileSize"`
}

func (c *client) ListStorageVols(ctx context.Context) ([]StorageVolInfo, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return []StorageVolInfo{}, err
	}

	pageIndex := 0
	const (
		pageSize = 100000
	)

	var storageVolsInfo []StorageVolInfo
	req := FileListReq{
		FilePath: c.completePath("") + "/",
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Errorf("parseBody json Marshal failed, error:%v", err)
		return storageVolsInfo, types.ErrorJsonMarshalFailed
	}

	for {
		reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/listLoginUserFile")
		var resp []FileListReply
		err := c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), &resp)
		if err != nil {
			return storageVolsInfo, err
		}

		for _, storageVol := range resp {
			if storageVol.FileType != types.StorageListTypeDir {
				continue
			}

			storageVolsInfo = append(storageVolsInfo, StorageVolInfo{
				Name: storageVol.FileName,
			})
		}

		//if resp.Total < pageSize {
		//	break
		//}
		pageIndex += pageSize

		//-todo not support page
		break
	}

	return storageVolsInfo, nil
}

func (c *client) statType(ctx context.Context, storageVolName, statKey string, isFolder bool) (FileInfo, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return FileInfo{}, err
	}

	if isFolder {
		statKey = strings.TrimSuffix(statKey, "/")
	}
	parentFolder := path.Dir(statKey)
	req := FileListReq{
		FilePath: c.completePath(storageVolName, parentFolder),
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Errorf("parseBody json Marshal failed, error:%v", err)
		return FileInfo{}, types.ErrorJsonMarshalFailed
	}

	for {
		reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/listLoginUserFile")
		var resp []FileListReply
		err := c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), &resp)
		if err != nil {
			return FileInfo{}, err
		}

		for _, item := range resp {
			if (isFolder && item.FileType == types.StorageListTypeDir) ||
				(!isFolder && item.FileType == types.StorageListTypeFile) {
				if item.FileName == path.Base(statKey) {
					return FileInfo{
						Key:  item.FileName,
						Size: item.FileSize,
					}, nil
				}
			}
		}

		//-todo not support page
		break
	}

	return FileInfo{}, errors.New("noSuchKey")
}

func (c *client) StatFile(ctx context.Context, storageVolName, filePath string) (FileInfo, error) {
	return c.statType(ctx, storageVolName, filePath, false)
}

func (c *client) UploadFile(ctx context.Context, storageVolName, filePath, digest string, totalLength int64, reader io.Reader) error {
	logger.Infof("uploadBigFile start, filePath:%s, digest:%s, totalLength:%d", path.Join(storageVolName, filePath), digest, totalLength)
	err := c.refreshToken(ctx)
	if err != nil {
		return err
	}

	fileName := path.Base(filePath)
	folderPath := path.Dir(filePath)
	pieceIdx := int64(0)
	reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/uploadBigUserFile")
	for {
		pieceIdx++
		rc := 0
		dataBuffer := make([]byte, 0)
		pipeBuffer := make([]byte, types.ReadBufferSize)
		for {
			n, err := reader.Read(pipeBuffer)
			if err != nil && err != io.EOF {
				logger.Errorf("read failed, error:%v", err)
				return err
			}
			if n == 0 {
				break
			}

			dataBuffer = append(dataBuffer, pipeBuffer[:n]...)
			rc += n
			if n < types.ReadBufferSize || rc >= types.ChunkSize {
				break
			}
		}
		if rc == 0 {
			break
		}

		r := &Reply{}
		response, err := c.httpClient.R().
			SetHeader("Content-Type", "application/octet-stream").
			SetHeader(types.AuthHeader, c.token).
			SetFileReader("file", fileName, bytes.NewBuffer(dataBuffer[:rc])).
			SetMultipartFormData(map[string]string{"filePath": c.completePath(storageVolName, folderPath), "index": fmt.Sprintf("%d", pieceIdx), "name": fileName}).
			SetResult(r).
			Post(c.endpoint + reqPath)
		if err != nil {
			logger.Errorf("UploadFile Post Error: %v, Body:%v", err, response.StatusCode())
			return err
		}
		if !response.IsSuccess() {
			r := &Reply{}
			err = json.Unmarshal(response.Body(), r)
			if err != nil {
				logger.Errorf("UploadFile json.Unmarshal Error: %v, Body:%v", err, response.StatusCode())
				return types.ErrorJsonUnmarshalFailed
			}

			return fmt.Errorf("http Code:%v, Code:%v, Msg:%v", response.Status(), r.Code, r.Msg)
		}

		if r.Code != http.StatusOK {
			return fmt.Errorf("code:%v, Msg:%v", r.Code, r.Msg)
		}

		// -fix bug
		//if rc < types.ChunkSize {
		//	break
		//}
	}

	//- merge file chunks
	type mergeChunkReq struct {
		FilePath string `json:"filePath"`
		Name     string `json:"name"`
	}

	req := mergeChunkReq{
		FilePath: c.completePath(storageVolName, folderPath),
		Name:     fileName,
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Errorf("parseBody json Marshal failed, error:%v", err)
		return types.ErrorJsonMarshalFailed
	}

	reqPath = fmt.Sprintf("/appspace/v1/system/bigfile/mergeBigUserFile")
	err = c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), nil)
	if err != nil {
		logger.Errorf("mergeChunks err:%v", err)
		return err
	}

	return nil
}

func (c *client) DownloadFile(ctx context.Context, storageVolName, filePath string) (io.ReadCloser, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return nil, err
	}

	reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/downloadBigUserFileStream?filePath=%s&name=%s",
		c.completePath(storageVolName, path.Dir(filePath)), path.Base(filePath))
	r := &Reply{}
	response, err := c.httpClient.R().
		SetHeader(types.AuthHeader, c.token).
		SetDoNotParseResponse(true).
		SetResult(r).
		Get(c.endpoint + reqPath)
	if err != nil {
		return nil, err
	}

	if !response.IsSuccess() {
		if response.StatusCode() != http.StatusOK {
			return nil, fmt.Errorf("http Code:%v, Status:%v", response.StatusCode(), response.Status())
		}

		r := &Reply{}
		err = json.Unmarshal(response.Body(), r)
		if err != nil {
			return nil, errors.New("internal error")
		}

		return nil, fmt.Errorf("code:%v, Msg:%v", r.Code, r.Msg)
	}

	return response.RawBody(), nil
}

func (c *client) RemoveFile(ctx context.Context, storageVolName, filePath string) error {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	type delReq struct {
		FilePath  string `json:"filePath"`
		LoginName string `json:"loginName"`
	}
	req := delReq{
		FilePath:  c.completePath(storageVolName, filePath),
		LoginName: types.AiCcLoginName,
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Errorf("parseBody json Marshal failed, error:%v", err)
		return types.ErrorJsonMarshalFailed
	}

	reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/delTempJobFile")
	err = c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) RemoveFiles(ctx context.Context, storageVolName string, objects []*FileInfo) error {
	for _, obj := range objects {
		err := c.RemoveFile(ctx, storageVolName, obj.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) RemoveFolder(ctx context.Context, storageVolName string, folderName string) error {
	return c.RemoveFile(ctx, storageVolName, folderName)
}

func (c *client) ListFiles(ctx context.Context, storageVolName, prefix, marker string, limit int64) ([]*FileInfo, error) {
	if prefix == "." || prefix == ".." {
		return nil, nil
	}

	if err := c.refreshToken(ctx); err != nil {
		return nil, nil
	}

	pageIndex := 0
	const (
		pageSize = 100000
	)

	req := FileListReq{
		FilePath: c.completePath(storageVolName, prefix),
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Errorf("parseBody json Marshal failed, error:%v", err)
		return nil, types.ErrorJsonMarshalFailed
	}

	var objects []*FileInfo
	for {
		var resp []FileListReply
		reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/listLoginUserFile")
		err := c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), &resp)
		if err != nil {
			return nil, err
		}

		for _, item := range resp {
			objects = append(objects, &FileInfo{
				Key:  item.FileName,
				Size: item.FileSize,
			})
		}

		//if resp.Total < pageSize {
		//	break
		//}
		pageIndex += pageSize

		//-todo not support page
		break
	}

	return objects, nil
}

func InSystemFileList(fileName string) bool {
	systemFiles := []string{
		".aipgarb",
		".aipclass"}

	for _, file := range systemFiles {
		if file == fileName {
			return true
		}
	}

	return false
}

func (c *client) listDirObjs(ctx context.Context, storageVolName, dirPath string) ([]*FileInfo, error) {
	if dirPath == "." || dirPath == ".." {
		return nil, nil
	}

	pageIndex := 0
	const (
		pageSize = 100000
	)

	req := FileListReq{
		FilePath: c.completePath(storageVolName, dirPath),
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Errorf("parseBody json Marshal failed, error:%v", err)
		return nil, types.ErrorJsonMarshalFailed
	}

	var objects []*FileInfo
	for {
		var resp []FileListReply
		reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/listLoginUserFile")
		err := c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), &resp)
		if err != nil {
			return nil, err
		}

		for _, item := range resp {
			key := path.Join(dirPath, item.FileName)
			if item.FileType == types.StorageListTypeDir {
				if !strings.HasSuffix(key, "/") {
					key += "/"
				}
			}

			if InSystemFileList(item.FileName) {
				continue
			}

			objects = append(objects, &FileInfo{
				Key:  key,
				Size: item.FileSize,
			})

			if item.FileType == types.StorageListTypeDir {
				tmpObjs, err := c.listDirObjs(ctx, storageVolName, key)
				if err != nil {
					return nil, err
				}

				objects = append(objects, tmpObjs...)
			}
		}

		//if resp.Total < pageSize {
		//	break
		//}
		pageIndex += pageSize

		//-todo not support page
		break
	}

	return objects, nil
}

func (c *client) ListDirFiles(ctx context.Context, storageVolName, prefix string) ([]*FileInfo, error) {
	if err := c.refreshToken(ctx); err != nil {
		return nil, err
	}

	resp, err := c.listDirObjs(ctx, storageVolName, prefix)
	if err != nil {
		return nil, err
	}

	// -fixme later
	if prefix != "" {
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		resp = append(resp, &FileInfo{
			Key: prefix,
		})
	}

	return resp, nil
}

func (c *client) IsFileExist(ctx context.Context, storageVolName, filePath string) (bool, error) {
	fileInfo, err := c.statType(ctx, storageVolName, filePath, false)
	if err != nil {
		return false, err
	}

	return fileInfo.Key == path.Base(filePath), nil
}

func (c *client) IsStorageVolExist(ctx context.Context, storageVolName string) (bool, error) {
	return c.StorageVolExists(ctx, storageVolName)
}

func (c *client) GetDownloadLink(ctx context.Context, storageVolName, filePath string, expire time.Duration) (string, error) {
	if err := c.refreshToken(ctx); err != nil {
		return "", err
	}

	signedUrl := fmt.Sprintf("%s@^^@%s/appspace/v1/system/bigfile/downloadBigUserFileStream?filePath=%s&name=%s",
		c.token, c.endpoint, c.completePath(storageVolName, path.Dir(filePath)), path.Base(filePath))
	return signedUrl, nil
}

func (c *client) CreateFolder(ctx context.Context, storageVolName, folderName string) error {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	type createReq struct {
		FilePath string `json:"filePath"`
	}

	req := createReq{
		FilePath: c.completePath(storageVolName, folderName),
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Errorf("parseBody json Marshal failed, error:%v", err)
		return types.ErrorJsonMarshalFailed
	}

	reqPath := fmt.Sprintf("/appspace/v1/system/bigfile/createUserJobFileDir")
	return c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), nil)
}

func (c *client) StatFolder(ctx context.Context, storageVolName, folderName string) (*FileInfo, bool, error) {
	statInfo, err := c.statType(ctx, storageVolName, folderName, true)
	if err != nil {
		return nil, false, err
	}

	return &statInfo, true, nil
}

func (c *client) PostTransfer(ctx context.Context, storageVolName, filePath string, isSuccess bool) error {
	return nil
}
