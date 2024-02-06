package badger

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

type DB struct {
	*badger.DB
}

type EntryOptions struct {
	TTL time.Duration
}

type ListResult struct {
	Key     string
	Size    int64
	Version uint64
	Meta    byte
}

func (l ListResult) String() string {
	return fmt.Sprintf("% -30s % 10d % 10d % 5s", l.Key, l.Size, l.Version, string(l.Meta))
}

func Open(dir string) (*DB, error) {
	opts := badger.DefaultOptions(dir)
	opts = opts.WithLogger(NewLogger())
	db, err := badger.Open(opts)
	return &DB{DB: db}, err
}

func jsonString(valueBytes []byte, valueJson any) (string, error) {
	if err := json.Unmarshal(valueBytes, &valueJson); err != nil {
		return "", err
	}
	indentJsonBytes, err := json.MarshalIndent(valueJson, "", "  ")
	if err != nil {
		return "", err
	}
	return string(indentJsonBytes), nil
}

func (db *DB) Get(key string, storageFormat string) (string, error) {
	valueBytes := make([]byte, 0)
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("Key %s not found", key)
			}
			return err
		}

		valueBytes, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	var value string

	switch storageFormat {
	case "json":
		var valueJson []map[string]interface{}
		value, err = jsonString(valueBytes, valueJson)
	case "string":
		value = string(valueBytes)
	case "int64AsBytes":
		valueInt := int(binary.BigEndian.Uint64(valueBytes))
		value = fmt.Sprintf("%d", valueInt)
	}

	return value, err
}

func (db *DB) List(prefix string, limit, offset int) ([]ListResult, int, error) {
	var keys []ListResult
	var total int

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		if limit > 0 {
			opts.PrefetchSize = limit
		}
		if prefix != "" {
			opts.Prefix = []byte(prefix)
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		currentOffset := 0
		for it.Rewind(); it.ValidForPrefix([]byte(prefix)); it.Next() {
			total++
			currentOffset++
			if currentOffset < offset {
				continue
			}

			if len(keys) < limit {
				item := it.Item()
				keys = append(
					keys,
					ListResult{
						Key:     string(item.KeyCopy(nil)),
						Size:    item.EstimatedSize(),
						Version: item.Version(),
						Meta:    item.UserMeta(),
					},
				)
			}
		}

		return nil
	})

	return keys, total, err
}

func (db *DB) Set(key, value string, opts *EntryOptions) error {
	return db.Update(func(txn *badger.Txn) error {
		if opts == nil {
			return txn.Set([]byte(key), []byte(value))
		}

		e := badger.NewEntry([]byte(key), []byte(value))
		if opts.TTL > 0 {
			e.WithTTL(opts.TTL)
		}
		return txn.SetEntry(e)
	})
}

func (db *DB) Delete(keys ...string) error {
	return db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete([]byte(key)); err != nil {
				return err
			}
		}

		return nil
	})
}
