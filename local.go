/*
Copyright The Helm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"context"
	"io"
	"os"

	pathutil "path"
	"path/filepath"
)

// LocalFilesystemBackend is a storage backend for local filesystem storage
type LocalFilesystemBackend struct {
	RootDirectory string
}

// NewLocalFilesystemBackend creates a new instance of LocalFilesystemBackend
func NewLocalFilesystemBackend(rootDirectory string) *LocalFilesystemBackend {
	absPath, err := filepath.Abs(rootDirectory)
	if err != nil {
		panic(err)
	}
	b := &LocalFilesystemBackend{RootDirectory: absPath}
	return b
}

// ListObjects lists all objects in root directory (depth 1)
func (b LocalFilesystemBackend) ListObjects(prefix string) ([]Object, error) {
	var objects []Object
	files, err := os.ReadDir(pathutil.Join(b.RootDirectory, prefix))
	if err != nil {
		if os.IsNotExist(err) { // OK if the directory doesnt exist yet
			err = nil
		}
		return objects, err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		info, err := f.Info()
		if err != nil {
			continue
		}
		object := Object{Path: f.Name(), Content: []byte{}, LastModified: info.ModTime()}
		objects = append(objects, object)
	}
	return objects, nil
}

// GetObject retrieves an object from root directory
func (b LocalFilesystemBackend) GetObject(path string) (Object, error) {
	var object Object
	object.Path = path
	fullpath := pathutil.Join(b.RootDirectory, path)
	content, err := os.ReadFile(fullpath)
	if err != nil {
		return object, err
	}
	object.Content = content
	info, err := os.Stat(fullpath)
	if err != nil {
		return object, err
	}
	object.LastModified = info.ModTime()
	return object, err
}

// PutObject puts an object in root directory
func (b LocalFilesystemBackend) PutObject(path string, content []byte) error {
	fullpath := pathutil.Join(b.RootDirectory, path)
	folderPath := pathutil.Dir(fullpath)
	_, err := os.Stat(folderPath)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(folderPath, 0774)
			if err != nil {
				return err
			}
			// os.MkdirAll set the dir permissions before the umask
			// we need to use os.Chmod to ensure the permissions of the created directory are 774
			// because the default umask will prevent that and cause the permissions to be 755
			err = os.Chmod(folderPath, 0774)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	err = os.WriteFile(fullpath, content, 0644)
	return err
}

func (b LocalFilesystemBackend) PutObjectStream(ctx context.Context, path string, content io.Reader) error {
	fullpath := pathutil.Join(b.RootDirectory, path)
	folderPath := pathutil.Dir(fullpath)

	if err := os.MkdirAll(folderPath, 0774); err != nil {
		return err
	}

	// check if context is already canceled
	if err := ctx.Err(); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(folderPath, "upload-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// use context-aware copy
	if _, err := copyWithContext(ctx, tmpFile, content); err != nil {
		return err
	}

	// Check context before final rename
	if err := ctx.Err(); err != nil {
		return err
	}

	return os.Rename(tmpFile.Name(), fullpath)
}

// copyWithContext copies from src to dst while respecting context cancellation
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64

	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}

		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			written += int64(nw)

			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}

		if er != nil {
			if er == io.EOF {
				break
			}
			return written, er
		}
	}
	return written, nil
}

// DeleteObject removes an object from root directory
func (b LocalFilesystemBackend) DeleteObject(path string) error {
	fullpath := pathutil.Join(b.RootDirectory, path)
	err := os.Remove(fullpath)
	return err
}
